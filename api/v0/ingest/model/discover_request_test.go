package model

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
)

func TestDiscoverRequest(t *testing.T) {
	const discoAddr = "td11223344"

	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	providerID, err := peer.IDFromPublicKey(pubKey)
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

	if discoReq.ProviderID != providerID {
		t.Error("wrong provider id")
	}
	if discoReq.DiscoveryAddr != discoAddr {
		t.Error("wrong discovery addr")
	}
}
