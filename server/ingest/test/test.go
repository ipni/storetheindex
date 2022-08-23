package test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/client"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

var rng = rand.New(rand.NewSource(1413))

//InitIndex initialize a new indexer engine.
func InitIndex(t *testing.T, withCache bool) indexer.Interface {
	valueStore, err := storethehash.New(context.Background(), t.TempDir(), nil)
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
			Allow:         false,
			Except:        []string{trustedID},
			Publish:       false,
			PublishExcept: []string{trustedID},
		},
		PollInterval:   config.Duration(time.Minute),
		RediscoverWait: config.Duration(time.Minute),
	}
	reg, err := registry.NewRegistry(context.Background(), discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func InitIngest(t *testing.T, indx indexer.Interface, reg *registry.Registry) *ingest.Ingester {
	cfg := config.NewIngest()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}

	ing, err := ingest.NewIngester(cfg, host, indx, reg, ds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ing.Close()
	})
	return ing
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
}

func IndexContent(t *testing.T, cl client.Ingest, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := util.RandomMultihashes(1, rng)

	contextID := []byte("test-context-id")
	metadata := []byte("test-metadata")

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
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

	expectValue := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadata,
	}
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

	mhs := util.RandomMultihashes(1, rng)

	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	addrs := []string{newAddr}

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], ctxID, metadata, addrs)
	if err != nil {
		t.Fatal(err)
	}

	info, allowed := reg.ProviderInfo(providerID)
	if info == nil {
		t.Fatal("did not get infor for provider:", providerID)
	}
	if !allowed {
		t.Fatal("provider not allowed")
	}

	maddr, err := multiaddr.NewMultiaddr(newAddr)
	if err != nil {
		t.Fatal(err)
	}

	if !info.AddrInfo.Addrs[0].Equal(maddr) {
		t.Fatalf("Did not update address.  Have %q, want %q", info.AddrInfo.Addrs[0].String(), maddr.String())
	}
}

func IndexContentFail(t *testing.T, cl client.Ingest, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := util.RandomMultihashes(1, rng)

	contextID := make([]byte, schema.MaxContextIDLen+1)
	metadata := []byte("test-metadata")

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	if !strings.HasSuffix(err.Error(), "context id too long") {
		t.Fatalf("expected error message: \"context id too long\", got %q", err.Error())
	}

	contextID = []byte("test-context-id")
	metadata = make([]byte, schema.MaxMetadataLen+1)
	err = cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	if err == nil {
		t.Fatal("expected error")
	}

	if !strings.HasSuffix(err.Error(), "metadata too long") {
		t.Fatalf("expected error message: \"metadata too long\", got %q", err.Error())
	}

	apierr, ok := err.(*v0.Error)
	if ok {
		if apierr.Status() != 400 {
			t.Fatalf("expected status 400, got %d", apierr.Status())
		}
	}
}

func AnnounceTest(t *testing.T, peerID peer.ID, cl client.Ingest) {
	ai, err := peer.AddrInfoFromString(fmt.Sprintf("/ip4/127.0.0.1/tcp/9999/p2p/%s", peerID))
	if err != nil {
		t.Fatal(err)
	}
	ai.ID = peerID

	mhs := util.RandomMultihashes(1, rng)

	if err := cl.Announce(context.Background(), ai, cid.NewCidV1(22, mhs[0])); err != nil {
		t.Fatalf("Failed to announce: %s", err)
	}

}
