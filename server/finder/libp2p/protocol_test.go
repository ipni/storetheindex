package p2pfinderserver_test

import (
	"context"
	"testing"

	indexer "github.com/filecoin-project/go-indexer-core"
	p2pclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/libp2p"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/providers"
	p2pserver "github.com/filecoin-project/storetheindex/server/finder/libp2p"
	"github.com/filecoin-project/storetheindex/server/finder/test"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

func setupServer(ctx context.Context, ind indexer.Interface, reg *providers.Registry, t *testing.T) (*libp2pserver.Server, host.Host) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	s := p2pserver.New(ctx, h, ind, reg)
	return s, h
}

func setupClient(ctx context.Context, peerID peer.ID, t *testing.T) (*p2pclient.Finder, host.Host) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := p2pclient.NewFinder(ctx, h, peerID)
	if err != nil {
		t.Fatal(err)
	}
	return c, h
}

func connect(ctx context.Context, t *testing.T, h1 host.Host, h2 host.Host) {
	if err := h1.Connect(ctx, *host.InfoFromHost(h2)); err != nil {
		t.Fatal(err)
	}
}

func TestFindIndexData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s, sh := setupServer(ctx, ind, reg, t)
	c, ch := setupClient(ctx, s.ID(), t)
	connect(ctx, t, ch, sh)
	test.FindIndexTest(ctx, t, c, ind, reg)
}
