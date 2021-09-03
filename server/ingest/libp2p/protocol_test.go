package p2pingestserver_test

import (
	"context"
	"testing"

	indexer "github.com/filecoin-project/go-indexer-core"
	p2pclient "github.com/filecoin-project/storetheindex/api/v0/ingest/client/libp2p"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/providers"
	p2pserver "github.com/filecoin-project/storetheindex/server/ingest/libp2p"
	"github.com/filecoin-project/storetheindex/server/ingest/test"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

var providerIdent = config.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

func setupServer(ctx context.Context, ind indexer.Interface, reg *providers.Registry, t *testing.T) (*libp2pserver.Server, host.Host) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	s := p2pserver.New(ctx, h, ind, reg)
	return s, h
}

func setupClient(ctx context.Context, peerID peer.ID, t *testing.T) (*p2pclient.Ingest, host.Host) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := p2pclient.NewIngest(ctx, h, peerID)
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

func TestRegisterProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t, providerIdent.PeerID)
	s, sh := setupServer(ctx, ind, reg, t)
	p2pClient, ch := setupClient(ctx, s.ID(), t)
	connect(ctx, t, ch, sh)

	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	test.RegisterProviderTest(t, p2pClient, providerIdent, addrs, reg)

	peerID, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		t.Fatal(err)
	}

	test.GetProviderTest(t, p2pClient, peerID)

	test.ListProvidersTest(t, p2pClient, peerID)

	test.IndexContent(t, p2pClient, providerIdent, ind)
}
