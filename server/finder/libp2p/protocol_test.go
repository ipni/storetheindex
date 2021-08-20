package p2pfinderserver_test

import (
	"context"
	"testing"

	indexer "github.com/filecoin-project/go-indexer-core"
	p2pclient "github.com/filecoin-project/storetheindex/api/v0/client/libp2p"
	"github.com/filecoin-project/storetheindex/internal/providers"
	p2pserver "github.com/filecoin-project/storetheindex/server/finder/libp2p"
	"github.com/filecoin-project/storetheindex/server/finder/test"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
)

func setupServer(ctx context.Context, ind indexer.Interface, reg *providers.Registry, t *testing.T) (*p2pserver.Server, host.Host) {
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	s, err := p2pserver.New(ctx, h, ind, reg)
	if err != nil {
		t.Fatal(err)
	}
	return s, h
}

func setupClient(ctx context.Context, peerID peer.ID, t *testing.T) (*p2pclient.Finder, host.Host) {
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
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

func TestGetCidData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s, sh := setupServer(ctx, ind, reg, t)
	c, ch := setupClient(ctx, s.ID(), t)
	connect(ctx, t, ch, sh)
	test.GetCidDataTest(ctx, t, c, ind, reg)
}
