package p2pprotocol_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/client"
	p2pclient "github.com/filecoin-project/storetheindex/client/libp2p/client"
	p2pserver "github.com/filecoin-project/storetheindex/client/libp2p/server"
	"github.com/filecoin-project/storetheindex/client/test"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"
)

func setupServer(ctx context.Context, ind *indexer.Engine, t *testing.T) (*p2pserver.Server, host.Host) {
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	s, err := p2pserver.New(ctx, h, ind)
	if err != nil {
		t.Fatal(err)
	}
	return s, h
}

func setupClient(ctx context.Context, t *testing.T) (client.Interface, host.Host) {
	h := bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport))
	c, err := p2pclient.New(ctx, h)
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
	c, ch := setupClient(ctx, t)
	s, sh := setupServer(ctx, ind, t)
	connect(ctx, t, ch, sh)
	test.GetCidDataTest(ctx, t, c, s, ind)
}

func TestEndpoint(t *testing.T) {
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	end, err := p2pclient.NewEndpoint("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}
	if end.Addr() != p {
		t.Fatal("wrong endpoint")
	}

	// This is a wrong peer.ID. We shouldn't generate endpoints for wrong peer.IDs.
	_, err = p2pclient.NewEndpoint("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1Bcv")
	if err == nil {
		t.Fatal("shouldn't have generated endpoint")
	}

}
