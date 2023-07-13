package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/admin/client"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/server/admin"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	peerIDStr     = "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ"
	indexerIDStr  = "12D3KooWQ9j3Ur5V9U63Vi6ved72TcA3sv34k74W3wpW5rwNvDc3"
	indexer2IDStr = "12D3KooWFhsKZsxo8sfs7zDcPRSwNnqo4vjBNn9fN25H3S1ZXGDq"
	serverIDStr   = "12D3KooWCAn6URUM34Z3APKrMFmd1mRkLWuPHvdJa3WwjAXbn58M"
)

type testenv struct {
	core     indexer.Interface
	ingester *ingest.Ingester
	registry *registry.Registry
	client   *client.Client
	server   *admin.Server
	errChan  chan error
}

var (
	peerID     peer.ID
	indexerID  peer.ID
	indexer2ID peer.ID
	serverID   peer.ID
)

func init() {
	var err error
	peerID, err = peer.Decode(peerIDStr)
	if err != nil {
		panic(err)
	}
	indexerID, err = peer.Decode(indexerIDStr)
	if err != nil {
		panic(err)
	}
	indexer2ID, err = peer.Decode(indexer2IDStr)
	if err != nil {
		panic(err)
	}
	serverID, err = peer.Decode(serverIDStr)
	if err != nil {
		panic(err)
	}
}

func TestAssign(t *testing.T) {
	te := makeTestenv(t)

	err := te.client.Assign(context.Background(), peerID)
	require.NoError(t, err)

	assigned, err := te.client.ListAssignedPeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, len(assigned))
	continuedFrom, ok := assigned[peerID]
	require.True(t, ok)
	require.Error(t, continuedFrom.Validate())

	err = te.client.Unassign(context.Background(), peerID)
	require.NoError(t, err)

	assigned, err = te.client.ListAssignedPeers(context.Background())
	require.NoError(t, err)
	require.Zero(t, len(assigned))

	te.close(t)
}

func TestHandoff(t *testing.T) {
	queriedFrozen := make(chan struct{})
	frozenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(queriedFrozen)
		defer req.Body.Close()

		adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
		provAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")

		frozenAddrInfo := peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{provAddr},
		}
		now := time.Now()

		responses := []model.ProviderInfo{{
			AddrInfo:              frozenAddrInfo,
			LastAdvertisement:     adCid,
			LastAdvertisementTime: now.Format(time.RFC3339),
			Publisher: &peer.AddrInfo{
				ID:    peerID,
				Addrs: []multiaddr.Multiaddr{provAddr},
			},
			FrozenAt:     adCid,
			FrozenAtTime: now.Format(time.RFC3339),
		}}

		data, err := json.Marshal(responses)
		if err != nil {
			panic(err.Error())
		}
		writeJsonResponse(w, http.StatusOK, data)
	}))
	defer frozenServer.Close()

	te := makeTestenv(t)

	err := te.client.Handoff(context.Background(), peerID, indexerID, frozenServer.URL)
	require.NoError(t, err)

	assigned, err := te.client.ListAssignedPeers(context.Background())
	require.NoError(t, err)
	select {
	case <-queriedFrozen:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to query frozen")
	}

	require.Equal(t, 1, len(assigned))
	continuedFrom, ok := assigned[peerID]
	require.True(t, ok)
	require.Equal(t, indexerID, continuedFrom)

	err = te.client.Handoff(context.Background(), peerID, indexerID, frozenServer.URL)
	require.ErrorContains(t, err, registry.ErrAlreadyAssigned.Error())

	te.close(t)
}

// TestHandoffNoPublisher tests reassigning a publisher when none of the
// providers on the frozen indexer has that publisher. This should result in
// successful handoff, but without reassigning the publisher
func TestHandoffNoPublisher(t *testing.T) {
	queriedFrozen := make(chan struct{})
	frozenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer close(queriedFrozen)
		defer req.Body.Close()

		adCid, _ := cid.Decode("bafybeigvgzoolc3drupxhlevdp2ugqcrbcsqfmcek2zxiw5wctk3xjpjwy")
		provAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")

		now := time.Now()

		responses := []model.ProviderInfo{{
			AddrInfo: peer.AddrInfo{
				ID:    peerID,
				Addrs: []multiaddr.Multiaddr{provAddr},
			},
			LastAdvertisement:     adCid,
			LastAdvertisementTime: now.Format(time.RFC3339),
			Publisher: &peer.AddrInfo{
				ID:    indexer2ID,
				Addrs: []multiaddr.Multiaddr{provAddr},
			},
			FrozenAt:     adCid,
			FrozenAtTime: now.Format(time.RFC3339),
		}}
		data, err := json.Marshal(responses)
		if err != nil {
			panic(err.Error())
		}
		writeJsonResponse(w, http.StatusOK, data)
	}))
	defer frozenServer.Close()

	te := makeTestenv(t)

	err := te.client.Handoff(context.Background(), peerID, indexerID, frozenServer.URL)
	require.NoError(t, err)

	assigned, err := te.client.ListAssignedPeers(context.Background())
	require.NoError(t, err)
	select {
	case <-queriedFrozen:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to query frozen")
	}

	require.Zero(t, len(assigned))
	te.close(t)
}

func TestStatus(t *testing.T) {
	te := makeTestenv(t)

	status, err := te.client.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, serverID, status.ID)
	require.False(t, status.Frozen)

	// Freeze indexer and see that status changes.
	err = te.client.Freeze(context.Background())
	require.NoError(t, err)
	status, err = te.client.Status(context.Background())
	require.NoError(t, err)
	require.True(t, status.Frozen)

	te.close(t)
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func makeTestenv(t *testing.T) *testenv {
	idx := initIndex(t, true)
	reg := initRegistry(t, peerIDStr)
	ing := initIngest(t, idx, reg)
	s := setupServer(t, idx, ing, reg)
	c := setupClient(t, s.URL())

	// Start server
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	return &testenv{
		core:     idx,
		registry: reg,
		ingester: ing,
		server:   s,
		client:   c,
		errChan:  errChan,
	}
}

func (te *testenv) close(t *testing.T) {
	require.NoError(t, te.server.Close())
	require.NoError(t, <-te.errChan)
	require.NoError(t, te.ingester.Close())
	require.NoError(t, te.core.Close())
	te.registry.Close()
}

func setupServer(t *testing.T, ind indexer.Interface, ing *ingest.Ingester, reg *registry.Registry) *admin.Server {
	reloadErrChan := make(chan chan error)
	s, err := admin.New("127.0.0.1:0", serverID, ind, ing, reg, reloadErrChan)
	require.NoError(t, err)
	return s
}

func setupClient(t *testing.T, host string) *client.Client {
	c, err := client.New(host)
	require.NoError(t, err)
	return c
}

func initRegistry(t *testing.T, trustedID string) *registry.Registry {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Allow:         false,
			Except:        []string{trustedID},
			Publish:       false,
			PublishExcept: []string{trustedID},
		},
		UseAssigner: true,
	}
	reg, err := registry.New(context.Background(), discoveryCfg, nil)
	require.NoError(t, err)
	return reg
}

func initIndex(t *testing.T, withCache bool) indexer.Interface {
	return engine.New(memory.New())
}

func initIngest(t *testing.T, indx indexer.Interface, reg *registry.Registry) *ingest.Ingester {
	cfg := config.NewIngest()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	dsTmp := dssync.MutexWrap(datastore.NewMapDatastore())
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)

	ing, err := ingest.NewIngester(cfg, host, indx, reg, ds, dsTmp)
	require.NoError(t, err)
	t.Cleanup(func() {
		ing.Close()
	})
	return ing
}
