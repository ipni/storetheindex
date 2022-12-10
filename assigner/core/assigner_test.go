package core_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/storetheindex/assigner/config"
	"github.com/ipni/storetheindex/assigner/core"
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

func TestAssignerAll(t *testing.T) {
	fakeIndexer1 := newTestIndexer(nil)
	defer fakeIndexer1.close()

	fakeIndexer2 := newTestIndexer(nil)
	defer fakeIndexer2.close()

	cfgAssignment := config.Assignment{
		// IndexerPool is the set of indexers the pool.
		IndexerPool: []config.Indexer{
			{
				AdminURL:    fakeIndexer1.adminServer.URL,
				IngestURL:   fakeIndexer1.ingestServer.URL,
				PresetPeers: []string{peer1IDStr, peer2IDStr},
			},
			{
				AdminURL:    fakeIndexer2.adminServer.URL,
				IngestURL:   fakeIndexer2.ingestServer.URL,
				PresetPeers: []string{peer1IDStr},
			},
		},
		Policy: config.Policy{
			Allow: true,
		},
		PubSubTopic: "testtopic",
		Replication: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assigner, err := core.NewAssigner(ctx, cfgAssignment, nil)
	require.NoError(t, err)
	require.True(t, assigner.InitDone())

	t.Log("Presets for", peer1IDStr, "=", assigner.Presets(peer1ID))
	require.Equal(t, []int{0, 1}, assigner.Presets(peer1ID))
	t.Log("Presets for", peer2IDStr, "=", assigner.Presets(peer2ID))
	require.Equal(t, []int{0}, assigner.Presets(peer2ID))

	assigned := assigner.Assigned(peer1ID)
	require.Equal(t, 2, len(assigned), "peer1 should be assigned to 2 indexers")

	assigned = assigner.Assigned(peer2ID)
	require.Zero(t, len(assigned), "peer2 should not be assigned to any indexers")

	counts := assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 1, counts[0])
	require.Equal(t, 1, counts[1])

	asmtChan, cancel := assigner.OnAssignment(peer2ID)
	defer cancel()

	// Send announce message for publisher peer2. It has a preset assignment to
	// indexer0, so should only be assigned to that indexer.
	adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	addrInfo := peer.AddrInfo{
		ID:    peer2ID,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = assigner.Announce(ctx, adCid, addrInfo)
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

	counts = assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 2, counts[0])
	require.Equal(t, 1, counts[1])

	asmtChan, cancel = assigner.OnAssignment(peer3ID)
	defer cancel()

	// Send announce message for publisher peer3. It has no preset assignment
	// so should be assigned to all indexers. Use a different CID because
	// already announced ads are ignored.
	adCid, _ = cid.Decode("QmNiV8rwXeC92hufGNu5qJ6L9AygrvDyi63gEpCQaqsE9B")
	addrInfo.ID = peer3ID
	err = assigner.Announce(ctx, adCid, addrInfo)
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

	counts = assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 3, counts[0])
	require.Equal(t, 2, counts[1])

	_, lateCancel := assigner.OnAssignment(peer2ID)
	require.NoError(t, assigner.Close())
	// Test that second close is OK.
	require.NoError(t, assigner.Close())
	// Test that cancel after close is ok.
	lateCancel()
}

func TestAssignerOne(t *testing.T) {
	fakeIndexer1 := newTestIndexer(nil)
	defer fakeIndexer1.close()

	fakeIndexer2 := newTestIndexer(nil)
	defer fakeIndexer2.close()

	cfgAssignment := config.Assignment{
		// IndexerPool is the set of indexers the pool.
		IndexerPool: []config.Indexer{
			{
				AdminURL:  fakeIndexer1.adminServer.URL,
				IngestURL: fakeIndexer1.ingestServer.URL,
			},
			{
				AdminURL:  fakeIndexer2.adminServer.URL,
				IngestURL: fakeIndexer2.ingestServer.URL,
			},
		},
		Policy: config.Policy{
			Allow: true,
		},
		PubSubTopic: "testtopic",
		Replication: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assigner, err := core.NewAssigner(ctx, cfgAssignment, nil)
	require.NoError(t, err)

	assigned := assigner.Assigned(peer1ID)
	require.Equal(t, 2, len(assigned), "peer1 should be assigned to 2 indexers")

	assigned = assigner.Assigned(peer2ID)
	require.Zero(t, len(assigned), "peer2 should not be assigned to any indexers")

	counts := assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 1, counts[0])
	require.Equal(t, 1, counts[1])

	asmtChan, cancel := assigner.OnAssignment(peer2ID)
	defer cancel()

	// Send announce for publisher peer2. It should be assigned to indexer 0.
	adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	addrInfo := peer.AddrInfo{
		ID:    peer2ID,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = assigner.Announce(ctx, adCid, addrInfo)
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

	counts = assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 2, counts[0])
	require.Equal(t, 1, counts[1])

	asmtChan, cancel = assigner.OnAssignment(peer3ID)
	defer cancel()

	// Send announce for publisher peer3. It should be assigned to indexer 1.
	adCid, _ = cid.Decode("QmNiV8rwXeC92hufGNu5qJ6L9AygrvDyi63gEpCQaqsE9B")
	addrInfo.ID = peer3ID
	err = assigner.Announce(ctx, adCid, addrInfo)
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
	require.Equal(t, 1, len(assigns))
	require.Equal(t, 1, assigns[0])

	counts = assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 2, counts[0])
	require.Equal(t, 2, counts[1])

	require.NoError(t, assigner.Close())
}

func TestAssignerPreferred(t *testing.T) {
	testAdminHandler0 := func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		if req.Method == "GET" {
			writeJsonResponse(w, http.StatusNoContent, nil)
		} else {
			writeJsonResponse(w, http.StatusOK, nil)
		}
	}

	testAdminHandler1 := func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		if req.Method == "GET" {
			switch req.URL.String() {
			case "/ingest/preferred":
				peers := []string{peer1IDStr, peer2IDStr, peer3IDStr}
				data, err := json.Marshal(peers)
				if err != nil {
					panic(err.Error())
				}
				writeJsonResponse(w, http.StatusOK, data)
			case "/ingest/assigned":
				writeJsonResponse(w, http.StatusNoContent, nil)
			default:
				http.Error(w, "", http.StatusNotFound)
			}
		} else {
			writeJsonResponse(w, http.StatusOK, nil)
		}
	}

	fakeIndexer1 := newTestIndexer(testAdminHandler0)
	defer fakeIndexer1.close()

	fakeIndexer2 := newTestIndexer(testAdminHandler1)
	defer fakeIndexer2.close()

	cfgAssignment := config.Assignment{
		// IndexerPool is the set of indexers the pool.
		IndexerPool: []config.Indexer{
			{
				AdminURL:  fakeIndexer1.adminServer.URL,
				IngestURL: fakeIndexer1.ingestServer.URL,
			},
			{
				AdminURL:  fakeIndexer2.adminServer.URL,
				IngestURL: fakeIndexer2.ingestServer.URL,
			},
		},
		Policy: config.Policy{
			Allow: true,
		},
		PubSubTopic: "testtopic",
		Replication: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assigner, err := core.NewAssigner(ctx, cfgAssignment, nil)
	require.NoError(t, err)

	assigned := assigner.Assigned(peer1ID)
	require.Zero(t, len(assigned), "peer1 should be assigned to 0 indexers")

	assigned = assigner.Assigned(peer2ID)
	require.Zero(t, len(assigned), "peer2 should be assigned to 0 indexers")

	counts := assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 0, counts[0])
	require.Equal(t, 0, counts[1])

	asmtChan, cancel := assigner.OnAssignment(peer2ID)
	defer cancel()

	// Send announce for publisher peer2. It should be assigned to indexer 0.
	adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	addrInfo := peer.AddrInfo{
		ID:    peer2ID,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = assigner.Announce(ctx, adCid, addrInfo)
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
	require.Equal(t, 1, assigns[0], "expected assignment to indexer 1")

	counts = assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 0, counts[0])
	require.Equal(t, 1, counts[1])

	asmtChan, cancel = assigner.OnAssignment(peer3ID)
	defer cancel()

	// Send announce for publisher peer3. It should be assigned to indexer 1.
	adCid, _ = cid.Decode("QmNiV8rwXeC92hufGNu5qJ6L9AygrvDyi63gEpCQaqsE9B")
	addrInfo.ID = peer3ID
	err = assigner.Announce(ctx, adCid, addrInfo)
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
	require.Equal(t, 1, len(assigns))
	require.Equal(t, 1, assigns[0], "expected assignment to indexer 1")

	counts = assigner.IndexerAssignedCounts()
	require.Equal(t, 2, len(counts))
	require.Equal(t, 0, counts[0])
	require.Equal(t, 2, counts[1])

	require.NoError(t, assigner.Close())
}

func TestPoolIndexerOffline(t *testing.T) {
	fakeIndexer1 := newTestIndexer(nil)
	defer fakeIndexer1.close()

	ready := make(chan struct{})
	fakeIndexer2 := newTestIndexer(func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()

		select {
		case <-ready:
		default:
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}

		if req.Method == "GET" {
			switch req.URL.String() {
			case "/ingest/assigned":
				peers := []string{peer1IDStr}
				data, err := json.Marshal(peers)
				if err != nil {
					panic(err.Error())
				}
				writeJsonResponse(w, http.StatusOK, data)
			case "/ingest/preferred":
				writeJsonResponse(w, http.StatusNoContent, nil)
			default:
				http.Error(w, "", http.StatusNotFound)
			}
		} else {
			writeJsonResponse(w, http.StatusOK, nil)
		}
	})
	defer fakeIndexer2.close()

	cfgAssignment := config.Assignment{
		// IndexerPool is the set of indexers the pool.
		IndexerPool: []config.Indexer{
			{
				AdminURL:  fakeIndexer1.adminServer.URL,
				IngestURL: fakeIndexer1.ingestServer.URL,
			},
			{
				AdminURL:  fakeIndexer2.adminServer.URL,
				IngestURL: fakeIndexer2.ingestServer.URL,
			},
		},
		Policy: config.Policy{
			Allow: true,
		},
		PubSubTopic: "testtopic",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Should not get all initial assignments.
	assigner, err := core.NewAssigner(ctx, cfgAssignment, nil)
	require.NoError(t, err)
	require.False(t, assigner.InitDone())

	// ----- Announce an unassigned publisher and check that it does not get assigned. -----

	asmtChan, cancel := assigner.OnAssignment(peer2ID)
	defer cancel()

	adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	addrInfo := peer.AddrInfo{
		ID:    peer2ID,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = assigner.Announce(ctx, adCid, addrInfo)
	require.NoError(t, err)

	timeout := time.NewTimer(2 * time.Second)
	select {
	case <-asmtChan:
		t.Fatal("shouold not see assignment with offline indexer")
	case <-timeout.C:
	}
	require.False(t, assigner.InitDone())

	// ----- Allow second indexer to work so that initialization can complete. ---
	close(ready)

	adCid, _ = cid.Decode("QmNiV8rwXeC92hufGNu5qJ6L9AygrvDyi63gEpCQaqsE9B")
	err = assigner.Announce(ctx, adCid, addrInfo)
	require.NoError(t, err)

	timeout.Reset(2 * time.Second)
	select {
	case <-asmtChan:
	case <-timeout.C:
		t.Fatal("timed out waiting for assignment")
	}
	timeout.Stop()

	require.True(t, assigner.InitDone())

	require.NoError(t, assigner.Close())
}

type testIndexer struct {
	adminServer  *httptest.Server
	ingestServer *httptest.Server
}

func defaultTestAdminHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if req.Method == "GET" {
		switch req.URL.String() {
		case "/ingest/assigned":
			peers := []string{peer1IDStr}
			data, err := json.Marshal(peers)
			if err != nil {
				panic(err.Error())
			}
			writeJsonResponse(w, http.StatusOK, data)
		case "/ingest/preferred":
			writeJsonResponse(w, http.StatusNoContent, nil)
		default:
			http.Error(w, "", http.StatusNotFound)
		}
	} else {
		writeJsonResponse(w, http.StatusOK, nil)
	}
}

func testIngestHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.WriteHeader(http.StatusOK)
}

func newTestIndexer(adminHandler func(http.ResponseWriter, *http.Request)) *testIndexer {
	ah := defaultTestAdminHandler
	if adminHandler != nil {
		ah = adminHandler
	}

	return &testIndexer{
		adminServer:  httptest.NewServer(http.HandlerFunc(ah)),
		ingestServer: httptest.NewServer(http.HandlerFunc(testIngestHandler)),
	}
}

func (ti *testIndexer) close() {
	ti.adminServer.Close()
	ti.ingestServer.Close()
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		http.Error(w, "", http.StatusInternalServerError)
	}
}
