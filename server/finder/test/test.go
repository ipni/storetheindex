package test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"runtime"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/storetheindex/api/v0/finder/client"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const providerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"

//InitIndex initialize a new indexer engine.
func InitIndex(t *testing.T, withCache bool) indexer.Interface {
	var err error
	var tmpDir string
	if runtime.GOOS == "windows" {
		tmpDir, err = ioutil.TempDir("", "sth_test")
		if err != nil {
			t.Fatal(err)
		}
	} else {
		tmpDir = t.TempDir()
	}
	valueStore, err := storethehash.New(tmpDir)
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
func InitRegistry(t *testing.T) *providers.Registry {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Action: "block",
			Trust:  []string{providerID},
		},
		PollInterval:   config.Duration(time.Minute),
		RediscoverWait: config.Duration(time.Minute),
	}
	reg, err := providers.NewRegistry(discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// PopulateIndex with some multihashes
func PopulateIndex(ind indexer.Interface, mhs []multihash.Multihash, v indexer.Value, t *testing.T) {
	err := ind.PutMany(mhs, v)
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

func FindIndexTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *providers.Registry) {
	// Generate some multihashes and populate indexer
	mhs, err := utils.RandomMultihashes(15)
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.Decode(providerID)
	if err != nil {
		t.Fatal(err)
	}
	v := indexer.MakeValue(p, 0, []byte(mhs[0]))
	PopulateIndex(ind, mhs[:10], v, t)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	info := &providers.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    p,
			Addrs: []multiaddr.Multiaddr{a},
		},
	}
	err = reg.Register(info)
	if err != nil {
		t.Fatal("could not register provider info:", err)
	}

	// Get single multihash
	resp, err := c.Find(ctx, mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Log("index values in resp:", len(resp.MultihashResults))
	err = checkResponse(resp, mhs[:1], []indexer.Value{v})
	if err != nil {
		t.Fatal(err)
	}

	// Get a batch of multihashes
	resp, err = c.FindBatch(ctx, mhs[:10])
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(resp, mhs[:10], []indexer.Value{v})
	if err != nil {
		t.Fatal(err)
	}

	// Get a batch of multihashes where only a subset is in the index
	resp, err = c.FindBatch(ctx, mhs)
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(resp, mhs[:10], []indexer.Value{v})
	if err != nil {
		t.Fatal(err)
	}

	// Get empty batch
	_, err = c.FindBatch(ctx, []multihash.Multihash{})
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(&models.FindResponse{}, []multihash.Multihash{}, []indexer.Value{})
	if err != nil {
		t.Fatal(err)
	}

	// Get batch with no multihashes in request
	_, err = c.FindBatch(ctx, mhs[10:])
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(&models.FindResponse{}, []multihash.Multihash{}, []indexer.Value{})
	if err != nil {
		t.Fatal(err)
	}
}

func checkResponse(r *models.FindResponse, mhs []multihash.Multihash, v []indexer.Value) error {
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
		if !utils.EqualValues(r.MultihashResults[i].Values, v) {
			return fmt.Errorf("wrong value included for a multihash: %s", v)
		}
	}
	// If there are any multihash responses, then there should be a provider
	if len(r.MultihashResults) != 0 && len(r.Providers) != 1 {
		return fmt.Errorf("wrong number of provider, expected 1 got %d", len(r.Providers))
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
