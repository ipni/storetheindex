package p2pfinderserver_test

import (
	"context"
	"testing"

	indexer "github.com/filecoin-project/go-indexer-core"
	p2pclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/libp2p"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/registry"
	p2pserver "github.com/filecoin-project/storetheindex/server/finder/libp2p"
	"github.com/filecoin-project/storetheindex/server/finder/test"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

func setupServer(ctx context.Context, ind indexer.Interface, reg *registry.Registry, t *testing.T) (*libp2pserver.Server, host.Host) {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	s := p2pserver.New(ctx, h, ind, reg)
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
	s, sh := setupServer(ctx, ind, reg, t)
	c := setupClient(s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}
	test.FindIndexTest(ctx, t, c, ind, reg)
}
