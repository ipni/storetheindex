package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/assigner/config"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	peer1IDStr = "12D3KooWQ9j3Ur5V9U63Vi6ved72TcA3sv34k74W3wpW5rwNvDc3"
	peer2IDStr = "12D3KooWFhsKZsxo8sfs7zDcPRSwNnqo4vjBNn9fN25H3S1ZXGDq"
	peer3IDStr = "12D3KooWCAn6URUM34Z3APKrMFmd1mRkLWuPHvdJa3WwjAXbn58M"
)

var (
	peer1ID, peer2ID, peer3ID peer.ID
)

func init() {
	var err error
	peer1ID, err = peer.Decode(peer1IDStr)
	if err != nil {
		panic(err)
	}
	peer2ID, err = peer.Decode(peer2IDStr)
	if err != nil {
		panic(err)
	}
	peer3ID, err = peer.Decode(peer3IDStr)
	if err != nil {
		panic(err)
	}
}

func TestNewAssigner(t *testing.T) {
	adminHandler := func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		peers := []string{peer1IDStr}
		data, err := json.Marshal(peers)
		if err != nil {
			panic(err.Error())
		}
		writeJsonResponse(w, http.StatusOK, data)

	}
	fakeIndexer1Admin := httptest.NewServer(http.HandlerFunc(adminHandler))
	defer fakeIndexer1Admin.Close()
	fakeIndexer2Admin := httptest.NewServer(http.HandlerFunc(adminHandler))
	defer fakeIndexer2Admin.Close()

	ingestHandler := func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		w.WriteHeader(http.StatusOK)
	}
	fakeIndexer1Ingest := httptest.NewServer(http.HandlerFunc(ingestHandler))
	defer fakeIndexer1Ingest.Close()
	fakeIndexer2Ingest := httptest.NewServer(http.HandlerFunc(ingestHandler))
	defer fakeIndexer2Ingest.Close()

	cfgAssignment := config.Assignment{
		// IndexerPool is the set of indexers the pool.
		IndexerPool: []config.Indexer{
			{
				AdminURL:    fakeIndexer1Admin.URL,
				IngestURL:   fakeIndexer1Ingest.URL,
				PresetPeers: []string{peer1IDStr, peer2IDStr},
			},
			{
				AdminURL:    fakeIndexer2Admin.URL,
				IngestURL:   fakeIndexer2Ingest.URL,
				PresetPeers: []string{peer1IDStr},
			},
		},
		Policy: config.Policy{
			Allow: true,
		},
		PubSubTopic: "testtopic",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assigner, err := NewAssigner(ctx, cfgAssignment, nil)
	require.NoError(t, err)

	t.Log("Presets for", peer1IDStr, "=", assigner.presets[peer1ID])
	require.Equal(t, []int{0, 1}, assigner.presets[peer1ID])
	t.Log("Presets for", peer2IDStr, "=", assigner.presets[peer2ID])
	require.Equal(t, []int{0}, assigner.presets[peer2ID])

	asmt, ok := assigner.assigned[peer1ID]
	require.True(t, ok)
	require.Equal(t, 2, len(asmt.indexers), "peer1 should be assigned to 2 indexers")

	asmt, ok = assigner.assigned[peer2ID]
	require.False(t, ok, "peer2 should not be assigned to any indexers")

	//showAssignments(t, assigner)

	asmtChan, cancel := assigner.OnAssignment(peer2ID)
	defer cancel()

	// Send announce message for publisher peer2. It has a preset assignment to
	// indexer1, so should only be assigned to that indexer.
	adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	addrs := []multiaddr.Multiaddr{a}
	err = assigner.receiver.Direct(ctx, adCid, peer2ID, addrs)
	require.NoError(t, err)

	var assignNum int
	var assigns []int
	timeout := time.NewTimer(3 * time.Second)
	open := true
	for open {
		select {
		case assignNum, open = <-asmtChan:
			if !open {
				break
			}
			t.Log("Publisher", peer2IDStr, "assigned to indexer", assignNum)
			assigns = append(assigns, assignNum)
		case <-timeout.C:
			t.Fatal("timed out waiting for assignment")
		}
	}
	if !timeout.Stop() {
		<-timeout.C
	}
	require.Equal(t, 1, len(assigns))
	require.Equal(t, 0, assigns[0])

	//showAssignments(t, assigner)

	asmtChan, cancel = assigner.OnAssignment(peer3ID)
	defer cancel()

	// Send announce message for publisher peer3. It has no preset assignment
	// so should be assigned to all indexers. Use a different CID because
	// already announced ads are ignored.
	adCid, _ = cid.Decode("QmNiV8rwXeC92hufGNu5qJ6L9AygrvDyi63gEpCQaqsE9B")
	err = assigner.receiver.Direct(ctx, adCid, peer3ID, addrs)
	require.NoError(t, err)

	assigns = assigns[:0]
	timeout.Reset(3 * time.Second)
	open = true
	for open {
		select {
		case assignNum, open = <-asmtChan:
			if !open {
				break
			}
			assigns = append(assigns, assignNum)
			t.Log("Publisher", peer3IDStr, "assigned to indexer", assignNum)
		case <-timeout.C:
			t.Fatal("timed out waiting for assignment")
		}
	}
	if !timeout.Stop() {
		<-timeout.C
	}
	sort.Ints(assigns)
	require.Equal(t, []int{0, 1}, assigns)

	//showAssignments(t, assigner)
}

func mockGetAssignments(ctx context.Context, adminURL string) ([]peer.ID, error) {
	return nil, nil
}

func showAssignments(t *testing.T, assigner *Assigner) {
	for peerID, asmt := range assigner.assigned {
		t.Log("Publisher", peerID, "assigned to indexers", asmt.indexers)
	}
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		log.Errorw("cannot write response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}
