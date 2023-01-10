package p2pfinderserver_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-datastore"
	indexer "github.com/ipni/go-indexer-core"
	p2pclient "github.com/ipni/storetheindex/api/v0/finder/client/libp2p"
	"github.com/ipni/storetheindex/internal/counter"
	"github.com/ipni/storetheindex/internal/registry"
	p2pserver "github.com/ipni/storetheindex/server/finder/libp2p"
	"github.com/ipni/storetheindex/server/finder/test"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func setupServer(ctx context.Context, ind indexer.Interface, reg *registry.Registry, idxCts *counter.IndexCounts, t *testing.T) (*p2pserver.FinderServer, host.Host) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	s := p2pserver.New(ctx, h, ind, reg, idxCts)
	return s, h
}

func setupClient(peerID peer.ID, t *testing.T) *p2pclient.Client {
	c, err := p2pclient.New(nil, peerID)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFindIndexData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s, sh := setupServer(ctx, ind, reg, nil, t)
	c := setupClient(s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}
	test.FindIndexTest(ctx, t, c, ind, reg)

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestFindIndexWithExtendedProviders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistryWithRestrictivePolicy(t, false)
	s, sh := setupServer(ctx, ind, reg, nil, t)
	c := setupClient(s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}
	test.ProvidersShouldBeUnaffectedByExtendedProvidersOfEachOtherTest(ctx, t, c, ind, reg)
	test.ExtendedProviderShouldHaveOwnMetadataTest(ctx, t, c, ind, reg)
	test.ExtendedProviderShouldInheritMetadataOfMainProviderTest(ctx, t, c, ind, reg)
	test.ContextualExtendedProvidersShouldUnionUpWithChainLevelOnesTest(ctx, t, c, ind, reg)
	test.ContextualExtendedProvidersShouldOverrideChainLevelOnesTest(ctx, t, c, ind, reg)
	test.MainProviderChainRecordIsIncludedIfItsMetadataIsDifferentTest(ctx, t, c, ind, reg)
	test.MainProviderContextRecordIsIncludedIfItsMetadataIsDifferentTest(ctx, t, c, ind, reg)

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestProviderInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	idxCts := counter.NewIndexCounts(datastore.NewMapDatastore())

	s, sh := setupServer(ctx, ind, reg, idxCts, t)
	p2pClient := setupClient(s.ID(), t)
	err := p2pClient.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}

	peerID := test.Register(ctx, t, reg)

	idxCts.AddCount(peerID, []byte("context-id"), 939)

	test.GetProviderTest(t, p2pClient, peerID)

	test.ListProvidersTest(t, p2pClient, peerID)

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestGetStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, false)
	defer ind.Close()
	reg := test.InitRegistry(t)
	defer reg.Close()
	s, sh := setupServer(ctx, ind, reg, nil, t)
	c := setupClient(s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}
	test.GetStatsTest(ctx, t, ind, s.RefreshStats, c)
}

func TestRemoveProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s, sh := setupServer(ctx, ind, reg, nil, t)
	c := setupClient(s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}

	test.RemoveProviderTest(ctx, t, c, ind, reg)

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}
