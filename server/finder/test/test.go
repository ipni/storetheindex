package test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/storetheindex/api/v0/finder/client"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const (
	providerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
	protocolID = 0x300000
)

var rng = rand.New(rand.NewSource(1413))

//InitIndex initialize a new indexer engine.
func InitIndex(t *testing.T, withCache bool) indexer.Interface {
	valueStore, err := storethehash.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return engine.New(resultCache, valueStore)
}

// InitRegistry initializes a new registry
func InitRegistry(t *testing.T) *registry.Registry {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Allow:       false,
			Except:      []string{providerID},
			Trust:       false,
			TrustExcept: []string{providerID},
		},
		PollInterval:   config.Duration(time.Minute),
		RediscoverWait: config.Duration(time.Minute),
	}
	reg, err := registry.NewRegistry(discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = reg.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return reg
}

// populateIndex with some multihashes
func populateIndex(ind indexer.Interface, mhs []multihash.Multihash, v indexer.Value, t *testing.T) {
	err := ind.Put(v, mhs...)
	if err != nil {
		t.Fatal("Error putting multihashes: ", err)
	}
	vals, ok, err := ind.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("index not found")
	}
	if len(vals) == 0 {
		t.Fatal("no values returned")
	}
	if !v.Equal(vals[0]) {
		t.Fatal("stored and retrieved values are different")
	}
}

func FindIndexTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	// Generate some multihashes and populate indexer
	mhs := util.RandomMultihashes(15, rng)
	p, err := peer.Decode(providerID)
	if err != nil {
		t.Fatal(err)
	}
	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	if err != nil {
		t.Fatal(err)
	}
	v := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs[:10], v, t)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	info := &registry.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    p,
			Addrs: []multiaddr.Multiaddr{a},
		},
	}
	err = reg.Register(ctx, info)
	if err != nil {
		t.Fatal("could not register provider info:", err)
	}

	// Get single multihash
	resp, err := c.Find(ctx, mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Log("index values in resp:", len(resp.MultihashResults))

	provResult := model.ProviderResult{
		ContextID: v.ContextID,
		Provider: peer.AddrInfo{
			ID:    v.ProviderID,
			Addrs: info.AddrInfo.Addrs,
		},
		Metadata: v.MetadataBytes,
	}

	expectedResults := []model.ProviderResult{provResult}
	err = checkResponse(resp, mhs[:1], expectedResults)
	if err != nil {
		t.Fatal(err)
	}

	// Get a batch of multihashes
	resp, err = c.FindBatch(ctx, mhs[:10])
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(resp, mhs[:10], expectedResults)
	if err != nil {
		t.Fatal(err)
	}

	// Get a batch of multihashes where only a subset is in the index
	resp, err = c.FindBatch(ctx, mhs)
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(resp, mhs[:10], expectedResults)
	if err != nil {
		t.Fatal(err)
	}

	// Get empty batch
	_, err = c.FindBatch(ctx, []multihash.Multihash{})
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(&model.FindResponse{}, []multihash.Multihash{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Get batch with no multihashes in request
	_, err = c.FindBatch(ctx, mhs[10:])
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(&model.FindResponse{}, []multihash.Multihash{}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func checkResponse(r *model.FindResponse, mhs []multihash.Multihash, expected []model.ProviderResult) error {
	// Check if everything was returned.
	if len(r.MultihashResults) != len(mhs) {
		return fmt.Errorf("number of values send in responses not correct, expected %d got %d", len(mhs), len(r.MultihashResults))
	}
	for i := range r.MultihashResults {
		// Check if multihash in list of multihashes
		if !hasMultihash(mhs, r.MultihashResults[i].Multihash) {
			return fmt.Errorf("multihash not found in response containing %d multihash", len(mhs))
		}

		// Check if same value
		for j, pr := range r.MultihashResults[i].ProviderResults {
			if !pr.Equal(expected[j]) {
				return fmt.Errorf("wrong ProviderResult included for a multihash: %s", expected[j])
			}
		}
	}
	return nil
}

func hasMultihash(mhs []multihash.Multihash, m multihash.Multihash) bool {
	for i := range mhs {
		if bytes.Equal([]byte(mhs[i]), []byte(m)) {
			return true
		}
	}
	return false
}

func GetProviderTest(t *testing.T, c client.Finder, providerID peer.ID) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provInfo, err := c.GetProvider(ctx, providerID)
	if err != nil {
		t.Fatal(err)
	}

	if provInfo == nil {
		t.Fatal("nil provider info")
	}
	if provInfo.AddrInfo.ID != providerID {
		t.Fatal("wrong peer id")
	}
}

func ListProvidersTest(t *testing.T, c client.Finder, providerID peer.ID) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	providers, err := c.ListProviders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 1 {
		t.Fatalf("should have 1 provider, has %d", len(providers))
	}
	if providers[0].AddrInfo.ID != providerID {
		t.Fatal("wrong peer id")
	}
}

func Register(ctx context.Context, t *testing.T, reg *registry.Registry) peer.ID {
	peerID, err := peer.Decode(providerID)
	if err != nil {
		t.Fatal(err)
	}

	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	if err != nil {
		t.Fatal(err)
	}
	info := &registry.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
	}

	err = reg.Register(ctx, info)
	if err != nil {
		t.Fatal(err)
	}

	return peerID
}
