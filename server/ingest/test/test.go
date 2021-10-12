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
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
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
func InitRegistry(t *testing.T, trustedID string) *registry.Registry {
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
	reg, err := registry.NewRegistry(discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// PopulateIndex with some multihashes
func PopulateIndex(ind indexer.Interface, mhs []multihash.Multihash, v indexer.Value, t *testing.T) {
	err := ind.Put(v, mhs...)
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

func RegisterProviderTest(t *testing.T, c client.Ingest, providerID peer.ID, privateKey crypto.PrivKey, addr string, reg *registry.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Log("registering provider")
	err := c.Register(ctx, providerID, privateKey, []string{addr})
	if err != nil {
		t.Fatal(err)
	}

	if !reg.IsRegistered(providerID) {
		t.Fatal("provider not registered")
	}

	// Test signature fail
	t.Log("registering provider with bad signature")
	badPeerID, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	if err != nil {
		t.Fatal(err)
	}

	err = c.Register(ctx, badPeerID, privateKey, []string{addr})
	if err == nil {
		t.Fatal("expected bad signature error")
	}

	// Test error if context canceled
	cancel()
	err = c.Register(ctx, providerID, privateKey, []string{addr})
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

func IndexContent(t *testing.T, cl client.Ingest, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs, err := util.RandomMultihashes(1)
	if err != nil {
		t.Fatal(err)
	}

	contextID := []byte("test-context-id")
	metadata := indexer.Metadata{0, []byte("hello")}

	err = cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
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

	expectValue := indexer.Value{providerID, contextID, metadata.Encode()}
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

func IndexContentNewAddr(t *testing.T, cl client.Ingest, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface, newAddr string, reg *registry.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs, err := util.RandomMultihashes(1)
	if err != nil {
		t.Fatal(err)
	}

	ctxID := []byte("test-context-id")
	metadata := indexer.Metadata{0, []byte("hello")}
	addrs := []string{newAddr}

	err = cl.IndexContent(ctx, providerID, privateKey, mhs[0], ctxID, metadata, addrs)
	if err != nil {
		t.Fatal(err)
	}

	info := reg.ProviderInfo(providerID)
	if info == nil {
		t.Fatal("did not get infor for provider:", providerID)
	}

	maddr, err := multiaddr.NewMultiaddr(newAddr)
	if err != nil {
		t.Fatal(err)
	}

	if !info.AddrInfo.Addrs[0].Equal(maddr) {
		t.Fatalf("Did not update address.  Have %q, want %q", info.AddrInfo.Addrs[0].String(), maddr.String())
	}
}
