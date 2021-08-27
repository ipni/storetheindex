package test

import (
	"context"
	"io/ioutil"
	"runtime"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/client"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

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
func InitRegistry(t *testing.T, trustedID string) *providers.Registry {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Action: "block",
			Trust:  []string{trustedID},
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

func RegisterProviderTest(t *testing.T, c client.Ingest, providerIdent config.Identity, addrs []string, reg *providers.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Log("registering provider")
	err := c.Register(ctx, providerIdent, addrs)
	if err != nil {
		t.Fatal(err)
	}

	p, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		t.Fatal(err)
	}

	if !reg.IsRegistered(p) {
		t.Fatal("provider not registered")
	}

	// Test signature fail
	t.Log("registering provider with bad signature")
	badIdent := providerIdent
	badIdent.PeerID = "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaXXX"
	err = c.Register(ctx, badIdent, addrs)
	if err == nil {
		t.Fatal("expected bad signature error")
	}

	// Test error if context canceled
	cancel()
	err = c.Register(ctx, providerIdent, addrs)
	if err == nil {
		t.Fatal("expected send to failed due to canceled context")
	}
}
