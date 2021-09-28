package model

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
)

func TestDiscoverRequest(t *testing.T) {
	const discoAddr = "td11223344"

	providerID, privKey, err := providerIdent.Decode()
	if err != nil {
		t.Fatal(err)
	}

	data, err := MakeDiscoverRequest(providerID, privKey, discoAddr)
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
