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
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

var ident = config.Identity{
	PeerID:  "12D3KooWPw6bfQbJHfKa2o5XpusChoq67iZoqgfnhecygjKsQRmG",
	PrivKey: "CAESQEQliDSXbU/zR4hrGNgAM0crtmxcZ49F3OwjmptYEFuU0b0TwLTJz/OlSBBuK7QDV2PiyGOCjDkyxSXymuqLu18=",
}

var providerID peer.ID

var hnd *httpHandler
var reg *providers.Registry

type mockIndexer struct {
	store map[cid.Cid][]indexer.Value
}

func (m *mockIndexer) Put(c cid.Cid, value indexer.Value) (bool, error) {
	vals, ok := m.store[c]
	if ok {
		for i := range vals {
			if value.Equal(vals[i]) {
				return false, nil
			}
		}
	}
	m.store[c] = append(vals, value)
	return true, nil
}

func (m *mockIndexer) Get(c cid.Cid) ([]indexer.Value, bool, error)         { return nil, false, nil }
func (m *mockIndexer) PutMany(cids []cid.Cid, value indexer.Value) error    { return nil }
func (m *mockIndexer) Remove(c cid.Cid, value indexer.Value) (bool, error)  { return false, nil }
func (m *mockIndexer) RemoveMany(cids []cid.Cid, value indexer.Value) error { return nil }
func (m *mockIndexer) RemoveProvider(providerID peer.ID) error              { return nil }
func (m *mockIndexer) Size() (int64, error)                                 { return 0, nil }

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
		store: map[cid.Cid][]indexer.Value{},
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
	c, err := cid.Decode("QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB")
	if err != nil {
		t.Fatal("cannot decode cid:", err)
	}
	metadata := []byte("hello world")

	data, err := models.MakeIngestRequest(ident, c, 0, metadata)
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
