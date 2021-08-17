package lotus

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

const testMinerAddr = "t01000"

func TestDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gateway := "wss://api.chain.love"
	disco, err := SetupGateway(ctx, gateway)
	if err != nil {
		t.Fatal(err)
	}

	var peerID peer.ID
	_, err = disco.Discover(ctx, peerID, testMinerAddr, nil, nil)
	if err == nil {
		t.Fatal("expected provider id mismatch error")
	}

	peerID, err = peer.Decode("12D3KooWGuQafP1HDkE2ixXZnX6q6LLygsUG1uoxaQEtfPAt5ygp")
	if err != nil {
		t.Fatal(err)
	}

	discovered, err := disco.Discover(ctx, peerID, testMinerAddr, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if discovered.AddrInfo.ID != peerID {
		t.Fatal("returned peer ID did not match requested")
	}

	t.Logf("Lotus discovered info for miner %q: %s", testMinerAddr, discovered.AddrInfo.String())
	err = disco.Close()
	if err != nil {
		t.Fatal(err)
	}
}
