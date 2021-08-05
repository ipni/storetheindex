package test

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/internal/finder"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

//InitIndex initialize a new indexer engine.
func InitIndex(t *testing.T, withCache bool) *indexer.Engine {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	var valueStore store.Interface

	valueStore, err = storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return indexer.NewEngine(resultCache, valueStore)
}

// InitRegistry initializes a new registry
func InitRegistry(t *testing.T) *providers.Registry {
	return providers.NewRegistry()
}

// PopulateIndex with some CIDs
func PopulateIndex(ind *indexer.Engine, cids []cid.Cid, e entry.Value, t *testing.T) {
	err := ind.PutMany(cids, e)
	if err != nil {
		t.Fatal("Error putting cids: ", err)
	}
}

func GetCidDataTest(ctx context.Context, t *testing.T, c finder.Interface, s finder.Server, ind *indexer.Engine) {
	// Generate some CIDs and populate indexer
	cids, err := test.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	e := entry.MakeValue(p, 0, cids[0].Bytes())
	PopulateIndex(ind, cids[:10], e, t)

	// Get single CID
	resp, err := c.Get(ctx, cids[0], s.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	checkResponse(resp, []cid.Cid{cids[0]}, []entry.Value{e}, t)

	// Get a batch of CIDs
	resp, err = c.GetBatch(ctx, cids[:10], s.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	checkResponse(resp, cids[:10], []entry.Value{e}, t)

	// Get a batch of CIDs where only a subset is in the index
	resp, err = c.GetBatch(ctx, cids, s.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	checkResponse(resp, cids[:10], []entry.Value{e}, t)

	// Get empty batch
	_, err = c.GetBatch(ctx, []cid.Cid{}, s.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	checkResponse(&models.Response{}, []cid.Cid{}, []entry.Value{}, t)

	// Get batch with no cids in request
	_, err = c.GetBatch(ctx, cids[10:], s.Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	checkResponse(&models.Response{}, []cid.Cid{}, []entry.Value{}, t)
}

func checkResponse(r *models.Response, cids []cid.Cid, e []entry.Value, t *testing.T) {
	// Check if everything was returned.
	if len(r.Cids) != len(cids) {
		t.Fatal("number of entries send in responses not correct")
	}
	for i := range r.Cids {
		// Check if cid in list of cids
		if !hasCid(cids, r.Cids[i].Cid) {
			t.Fatal("cid not found in response")
		}

		// Check if same entry
		if !utils.EqualEntries(r.Cids[i].Entries, e) {
			t.Fatal("wrong entry included for a cid")
		}
	}
}

func hasCid(cids []cid.Cid, c cid.Cid) bool {
	for i := range cids {
		if cids[i] == c {
			return true
		}
	}
	return false
}
