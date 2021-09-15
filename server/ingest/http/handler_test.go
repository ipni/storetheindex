package httpingestserver

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

var ident = config.Identity{
	PeerID:  "12D3KooWPw6bfQbJHfKa2o5XpusChoq67iZoqgfnhecygjKsQRmG",
	PrivKey: "CAESQEQliDSXbU/zR4hrGNgAM0crtmxcZ49F3OwjmptYEFuU0b0TwLTJz/OlSBBuK7QDV2PiyGOCjDkyxSXymuqLu18=",
}

var providerID peer.ID

var hnd *httpHandler
var reg *providers.Registry

type mockIndexer struct {
	store map[string][]indexer.Value
}

func (m *mockIndexer) Put(mh multihash.Multihash, value indexer.Value) (bool, error) {
	k := mh.B58String()
	vals, ok := m.store[k]
	if ok {
		for i := range vals {
			if value.Equal(vals[i]) {
				return false, nil
			}
		}
	}
	m.store[k] = append(vals, value)
	return true, nil
}

func (m *mockIndexer) Get(mh multihash.Multihash) ([]indexer.Value, bool, error) {
	return nil, false, nil
}
func (m *mockIndexer) PutMany(mhs []multihash.Multihash, value indexer.Value) error { return nil }
func (m *mockIndexer) Remove(mh multihash.Multihash, value indexer.Value) (bool, error) {
	return false, nil
}
func (m *mockIndexer) RemoveMany(mhs []multihash.Multihash, value indexer.Value) error { return nil }
func (m *mockIndexer) RemoveProvider(providerID peer.ID) error                         { return nil }
func (m *mockIndexer) Size() (int64, error)                                            { return 0, nil }

func init() {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Action: "block",
			Trust:  []string{ident.PeerID},
		},
	}

	var err error
	reg, err = providers.NewRegistry(discoveryCfg, nil, nil)
	if err != nil {
		panic(err)
	}

	idx := &mockIndexer{
		store: map[string][]indexer.Value{},
	}
	hnd = newHandler(idx, reg)

	providerID, err = peer.Decode(ident.PeerID)
	if err != nil {
		panic("Could not decode peer ID")
	}
}

func TestRegisterProvider(t *testing.T) {
	data, err := models.MakeRegisterRequest(ident, nil)
	if err != nil {
		t.Fatal(err)
	}
	reqBody := bytes.NewBuffer(data)

	req := httptest.NewRequest("POST", "http://example.com/providers", reqBody)
	w := httptest.NewRecorder()
	hnd.RegisterProvider(w, req)

	resp := w.Result()

	if resp.StatusCode != http.StatusOK {
		t.Fatal("expected response to be", http.StatusOK)
	}

	pinfo := reg.ProviderInfo(providerID)
	if pinfo == nil {
		t.Fatal("provider was not registered")
	}
}

func TestIndexContent(t *testing.T) {
	m, err := multihash.FromB58String("QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB")
	if err != nil {
		t.Fatal(err)
	}
	metadata := []byte("hello world")

	data, err := models.MakeIngestRequest(ident, m, 0, metadata)
	if err != nil {
		t.Fatal(err)
	}
	reqBody := bytes.NewBuffer(data)

	req := httptest.NewRequest("POST", "http://example.com/providers", reqBody)
	w := httptest.NewRecorder()
	hnd.IndexContent(w, req)

	resp := w.Result()

	if resp.StatusCode != http.StatusOK {
		t.Fatal("expected response to be", http.StatusOK)
	}
}
