package model

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func TestRegisterRequest(t *testing.T) {
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	data, err := MakeRegisterRequest(peerID, privKey, addrs)
	if err != nil {
		t.Fatal(err)
	}

	peerRec, err := ReadRegisterRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	seq0 := peerRec.Seq

	data, err = MakeRegisterRequest(peerID, privKey, addrs)
	if err != nil {
		t.Fatal(err)
	}
	peerRec, err = ReadRegisterRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	if seq0 >= peerRec.Seq {
		t.Fatal("sequence not greater than last seen")
	}
}
