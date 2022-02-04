package httpingestserver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/model"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

const testProtocolID = 0x300000

var ident = config.Identity{
	PeerID:  "12D3KooWPw6bfQbJHfKa2o5XpusChoq67iZoqgfnhecygjKsQRmG",
	PrivKey: "CAESQEQliDSXbU/zR4hrGNgAM0crtmxcZ49F3OwjmptYEFuU0b0TwLTJz/OlSBBuK7QDV2PiyGOCjDkyxSXymuqLu18=",
}

var providerID peer.ID

var hnd *httpHandler
var reg *registry.Registry

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
			for i := range vals {
				if value.Match(vals[i]) {
					return nil
				}
			}
		}
		m.store[k] = append(vals, value)
	}
	return nil
}

func (m *mockIndexer) Remove(indexer.Value, ...multihash.Multihash) error { return nil }
func (m *mockIndexer) RemoveProvider(peer.ID) error                       { return nil }
func (m *mockIndexer) RemoveProviderContext(peer.ID, []byte) error        { return nil }
func (m *mockIndexer) Size() (int64, error)                               { return 0, nil }
func (m *mockIndexer) Flush() error                                       { return nil }
func (m *mockIndexer) Close() error                                       { return nil }
func (m *mockIndexer) Iter() (indexer.Iterator, error)                    { return nil, nil }

func init() {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Allow:       false,
			Except:      []string{ident.PeerID},
			Trust:       false,
			TrustExcept: []string{ident.PeerID},
		},
	}

	var err error
	reg, err = registry.NewRegistry(context.Background(), discoveryCfg, nil, nil)
	if err != nil {
		panic(err)
	}

	idx := &mockIndexer{
		store: map[string][]indexer.Value{},
	}

	host, err := libp2p.New()
	if err != nil {
		panic(err)
	}
	ds := datastore.NewMapDatastore()
	ing, err := ingest.NewIngester(config.Ingest{}, host, idx, reg, ds)
	if err != nil {
		panic(err)
	}
	hnd = newHandler(idx, ing, reg)

	providerID, err = peer.Decode(ident.PeerID)
	if err != nil {
		panic("Could not decode peer ID")
	}
}

func TestRegisterProvider(t *testing.T) {
	peerID, privKey, err := ident.Decode()
	if err != nil {
		t.Fatal(err)
	}

	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	data, err := model.MakeRegisterRequest(peerID, privKey, addrs)
	if err != nil {
		t.Fatal(err)
	}
	reqBody := bytes.NewBuffer(data)

	req := httptest.NewRequest(http.MethodPost, "http://example.com/providers", reqBody)
	w := httptest.NewRecorder()
	hnd.registerProvider(w, req)

	resp := w.Result()

	if resp.StatusCode != http.StatusOK {
		t.Fatal("expected response to be", http.StatusOK)
	}

	pinfo := reg.ProviderInfo(peerID)
	if pinfo == nil {
		t.Fatal("provider was not registered")
	}
}
