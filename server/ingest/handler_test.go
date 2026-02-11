package ingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/ingest/model"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var ident = config.Identity{
	PeerID:  "12D3KooWPw6bfQbJHfKa2o5XpusChoq67iZoqgfnhecygjKsQRmG",
	PrivKey: "CAESQEQliDSXbU/zR4hrGNgAM0crtmxcZ49F3OwjmptYEFuU0b0TwLTJz/OlSBBuK7QDV2PiyGOCjDkyxSXymuqLu18=",
}

type mockIndexer struct {
	store map[string][]indexer.Value
}

func (m *mockIndexer) Get(mh multihash.Multihash) ([]indexer.Value, bool, error) {
	return nil, false, nil
}
func (m *mockIndexer) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	for _, mh := range mhs {
		k := mh.B58String()
		vals, ok := m.store[k]
		if ok {
			if slices.ContainsFunc(vals, value.Match) {
				return nil
			}
		}
		m.store[k] = append(vals, value)
	}
	return nil
}

func (m *mockIndexer) Remove(indexer.Value, ...multihash.Multihash) error { return nil }
func (m *mockIndexer) RemoveProvider(context.Context, peer.ID) error      { return nil }
func (m *mockIndexer) RemoveProviderContext(peer.ID, []byte) error        { return nil }
func (m *mockIndexer) Size() (int64, error)                               { return 0, nil }
func (m *mockIndexer) Flush() error                                       { return nil }
func (m *mockIndexer) Close() error                                       { return nil }
func (m *mockIndexer) Stats() (*indexer.Stats, error)                     { return nil, indexer.ErrStatsNotSupported }

func TestHandleRegisterProvider(t *testing.T) {
	discoveryCfg := config.Discovery{
		Policy: config.Policy{
			Allow:         false,
			Except:        []string{ident.PeerID},
			Publish:       true,
			PublishExcept: []string{ident.PeerID},
		},
	}

	reg, err := registry.New(context.Background(), discoveryCfg, nil)
	require.NoError(t, err)
	defer reg.Close()

	idx := &mockIndexer{
		store: map[string][]indexer.Value{},
	}

	host, err := libp2p.New()
	require.NoError(t, err)
	defer host.Close()

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	dsTmp := dssync.MutexWrap(datastore.NewMapDatastore())
	ing, err := ingest.NewIngester(config.NewIngest(), host, idx, reg, ds, dsTmp)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ing.Close())
	})

	s, err := New("127.0.0.1:", idx, ing, reg)
	require.NoError(t, err)

	peerID, privKey, err := ident.Decode()
	require.NoError(t, err)

	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	data, err := model.MakeRegisterRequest(peerID, privKey, addrs)
	require.NoError(t, err)
	reqBody := bytes.NewBuffer(data)

	req := httptest.NewRequest(http.MethodPost, "http://example.com/providers", reqBody)
	w := httptest.NewRecorder()
	s.postRegisterProvider(w, req)

	resp := w.Result()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	pinfo, allowed := reg.ProviderInfo(peerID)
	require.NotNil(t, pinfo, "provider was not registered")
	require.True(t, allowed, "provider not allowed")
}
