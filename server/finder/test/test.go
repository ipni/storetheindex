package test

import (
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
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/filecoin-project/storetheindex/api/v0/client"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
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

// PopulateIndex with some CIDs
func PopulateIndex(ind indexer.Interface, cids []cid.Cid, v indexer.Value, t *testing.T) {
	err := ind.PutMany(cids, v)
	if err != nil {
		t.Fatal("Error putting cids: ", err)
	}
	vals, ok, err := ind.Get(cids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cid not found")
	}
	if len(vals) == 0 {
		t.Fatal("no values returned")
	}
	if !v.Equal(vals[0]) {
		t.Fatal("stored and retrieved values are different")
	}
}

func GetCidDataTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *providers.Registry) {
	// Generate some CIDs and populate indexer
	cids, err := test.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.Decode(providerID)
	if err != nil {
		t.Fatal(err)
	}
	v := indexer.MakeValue(p, 0, cids[0].Bytes())
	PopulateIndex(ind, cids[:10], v, t)

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

	// Get single CID
	resp, err := c.Get(ctx, cids[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Log("cids in resp:", len(resp.CidResults))
	err = checkResponse(resp, []cid.Cid{cids[0]}, []indexer.Value{v})
	if err != nil {
		t.Fatal(err)
	}

	// Get a batch of CIDs
	resp, err = c.GetBatch(ctx, cids[:10])
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(resp, cids[:10], []indexer.Value{v})
	if err != nil {
		t.Fatal(err)
	}

	// Get a batch of CIDs where only a subset is in the index
	resp, err = c.GetBatch(ctx, cids)
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(resp, cids[:10], []indexer.Value{v})
	if err != nil {
		t.Fatal(err)
	}

	// Get empty batch
	_, err = c.GetBatch(ctx, []cid.Cid{})
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(&models.Response{}, []cid.Cid{}, []indexer.Value{})
	if err != nil {
		t.Fatal(err)
	}

	// Get batch with no cids in request
	_, err = c.GetBatch(ctx, cids[10:])
	if err != nil {
		t.Fatal(err)
	}
	err = checkResponse(&models.Response{}, []cid.Cid{}, []indexer.Value{})
	if err != nil {
		t.Fatal(err)
	}
}

func checkResponse(r *models.Response, cids []cid.Cid, v []indexer.Value) error {
	// Check if everything was returned.
	if len(r.CidResults) != len(cids) {
		return fmt.Errorf("number of values send in responses not correct, expected %d got %d", len(cids), len(r.CidResults))
	}
	for i := range r.CidResults {
		// Check if cid in list of cids
		if !hasCid(cids, r.CidResults[i].Cid) {
			return fmt.Errorf("cid not found in response containing %d cids", len(cids))
		}

		// Check if same value
		if !utils.EqualValues(r.CidResults[i].Values, v) {
			return fmt.Errorf("wrong value included for a cid: %s", v)
		}
	}
	// If there are any CID responses, then there should be a provider
	if len(r.CidResults) != 0 && len(r.Providers) != 1 {
		return fmt.Errorf("wrong number of provider, expected 1 got %d", len(r.Providers))
	}
	return nil
}

func hasCid(cids []cid.Cid, c cid.Cid) bool {
	for i := range cids {
		if cids[i] == c {
			return true
		}
	}
	return false
}
