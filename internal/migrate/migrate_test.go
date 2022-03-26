package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
	providerID    = "12D3KooWKSNuuq77xqnpPLnU3fq1bTQW2TwSZL2Z4QTHEYpUVzfr"
	providerAddr  = "/ip4/127.0.0.1/tcp/9999"
	provider2ID   = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
	provider2Addr = "/ip4/127.0.0.2/tcp/7777"

	publisherID   = "12D3KooWFNkdmdb38g4VVCGaJsKin4BzGpP4bedfxU76PF2AahoP"
	publisherAddr = "/ip4/127.0.0.3/tcp/1234"
)

func createRegistryData(dstore datastore.Datastore) error {
	r, err := registry.NewRegistry(config.NewDiscovery(), dstore, nil)
	if err != nil {
		return err
	}
	if err = r.Start(context.Background()); err != nil {
		return err
	}

	provID, err := peer.Decode(providerID)
	if err != nil {
		return err
	}
	provAddr, err := multiaddr.NewMultiaddr(providerAddr)
	if err != nil {
		return err
	}
	pubID, err := peer.Decode(publisherID)
	if err != nil {
		return err
	}
	pubAddr, err := multiaddr.NewMultiaddr(publisherAddr)
	if err != nil {
		return err
	}

	info := &registry.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    provID,
			Addrs: []multiaddr.Multiaddr{provAddr},
		},
		Publisher: &peer.AddrInfo{
			ID:    pubID,
			Addrs: []multiaddr.Multiaddr{pubAddr},
		},
	}

	if err = r.Register(context.Background(), info); err != nil {
		return err
	}

	provID, err = peer.Decode(provider2ID)
	if err != nil {
		return err
	}
	provAddr, err = multiaddr.NewMultiaddr(provider2Addr)
	if err != nil {
		return err
	}
	info = &registry.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    provID,
			Addrs: []multiaddr.Multiaddr{provAddr},
		},
		Publisher: &peer.AddrInfo{
			ID:    provID,
			Addrs: []multiaddr.Multiaddr{provAddr},
		},
	}
	return r.Register(context.Background(), info)
}

func TestRevertMigrate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dstore, err := leveldb.NewDatastore(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Empty db needs migration (needs version written).
	need, err := NeedMigration(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if !need {
		t.Fatal("should have reported migration needed")
	}

	// Write db version.
	ok, err := Migrate(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("did not migrate empty datastore")
	}

	if err = createRegistryData(dstore); err != nil {
		t.Fatal(err)
	}

	// Should not need migration any more
	need, err = NeedMigration(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if need {
		t.Fatal("should have reported migration not needed")
	}

	ok, err = Revert(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("did not revert datastore")
	}

	// Revert again should do nothing.
	ok, err = Revert(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have reverted again")
	}

	// Should need migration now.
	need, err = NeedMigration(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if !need {
		t.Fatal("should have reported migration needed")
	}

	// Migrade datastore.
	ok, err = Migrate(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("did not migrate datastore")
	}

	// Migrade again should do nothing.
	ok, err = Migrate(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have migrated again")
	}

	// Should not need migration now.
	need, err = NeedMigration(ctx, dstore)
	if err != nil {
		t.Fatal(err)
	}
	if need {
		t.Fatal("should have reported migration not needed")
	}

	// Check that registry can read in values.
	r, err := registry.NewRegistry(config.NewDiscovery(), dstore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(ctx); err != nil {
		t.Fatal(err)
	}

	providers := r.AllProviderInfo()
	if len(providers) != 2 {
		t.Fatalf("expected 2 provider info, got %d", len(providers))
	}

	info := providers[0]
	if info.AddrInfo.ID.String() != providerID {
		t.Fatal("wrong provider id")
	}
	if len(info.AddrInfo.Addrs) != 1 {
		t.Fatalf("expected 1 provider addr, got %d", len(info.AddrInfo.Addrs))
	}
	if info.Publisher == nil {
		t.Fatal("missing provider1 publisher")
	}
	if info.Publisher.ID.String() != publisherID {
		t.Fatal("wrong publisher id")
	}
	if len(info.Publisher.Addrs) != 0 {
		t.Fatalf("expected 0 publisher addr, got %d", len(info.Publisher.Addrs))
	}

	info = providers[1]
	if info.AddrInfo.ID.String() != provider2ID {
		t.Fatal("wrong provider2 id")
	}
	if len(info.AddrInfo.Addrs) != 1 {
		t.Fatalf("expected 1 provider2 addr, got %d", len(info.AddrInfo.Addrs))
	}
	if info.Publisher == nil {
		t.Fatal("missing provider2 publisher")
	}
	if info.Publisher.ID.String() != provider2ID {
		t.Fatal("wrong publisher id")
	}
	if len(info.Publisher.Addrs) != 1 {
		t.Fatalf("expected 1 publisher addr, got %d", len(info.Publisher.Addrs))
	}
	if !info.Publisher.Addrs[0].Equal(info.AddrInfo.Addrs[0]) {
		t.Fatal("expected publisher addr to be same as provider addr")
	}

	if err = r.Close(); err != nil {
		t.Fatalf("failed to close registry: %s", err)
	}
	if err = dstore.Close(); err != nil {
		t.Fatalf("failed to close datastore: %s", err)
	}
}
