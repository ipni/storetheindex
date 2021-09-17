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
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
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
			Allow:       false,
			Except:      []string{trustedID},
			Trust:       false,
			TrustExcept: []string{trustedID},
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
		t.Fatal("multihash not found")
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
	err := c.Register(ctx, providerIdent.PeerID, providerIdent.PrivKey, addrs)
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
	err = c.Register(ctx, badIdent.PeerID, badIdent.PrivKey, addrs)
	if err == nil {
		t.Fatal("expected bad signature error")
	}

	// Test error if context canceled
	cancel()
	err = c.Register(ctx, providerIdent.PeerID, providerIdent.PrivKey, addrs)
	if err == nil {
		t.Fatal("expected send to failed due to canceled context")
	}
}

func GetProviderTest(t *testing.T, c client.Ingest, providerID peer.ID) {
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

func ListProvidersTest(t *testing.T, c client.Ingest, providerID peer.ID) {
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

func IndexContent(t *testing.T, cl client.Ingest, providerIdent config.Identity, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs, err := utils.RandomMultihashes(1)
	if err != nil {
		t.Fatal(err)
	}

	metadata := []byte("hello")

	err = cl.IndexContent(ctx, providerIdent.PeerID, providerIdent.PrivKey, mhs[0], 0, metadata)
	if err != nil {
		t.Fatal(err)
	}

	vals, ok, err := ind.Get(mhs[0])
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("did not find content")
	}
	if len(vals) == 0 {
		t.Fatal("no content values returned")
	}

	p, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		t.Fatal(err)
	}

	expectValue := indexer.MakeValue(p, 0, metadata)
	ok = false
	for i := range vals {
		if vals[i].Equal(expectValue) {
			ok = true
			break
		}
	}
	if !ok {
		t.Fatal("did not get expected content")
	}
}
