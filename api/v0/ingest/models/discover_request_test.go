package models

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestDiscoverRequest(t *testing.T) {
	const discoAddr = "td11223344"

	data, err := MakeDiscoverRequest(providerIdent, discoAddr)
	if err != nil {
		t.Fatal(err)
	}

	discoReq, err := ReadDiscoverRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	peerID, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		t.Fatal(err)
	}

	if discoReq.ProviderID != peerID {
		t.Error("wrong provider id")
	}
	if discoReq.DiscoveryAddr != discoAddr {
		t.Error("wrong discovery addr")
	}
}
