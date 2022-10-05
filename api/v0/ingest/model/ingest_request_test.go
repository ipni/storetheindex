package model

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

var rng = rand.New(rand.NewSource(1413))

func TestIngestRequest(t *testing.T) {
	mhs := util.RandomMultihashes(1, rng)

	metadata := []byte("test-metadata")

	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		t.Fatal(err)
	}

	ctxID := []byte("test-context-id")
	address := "/ip4/127.0.0.1/tcp/7777"
	data, err := MakeIngestRequest(peerID, privKey, mhs[0], ctxID, metadata, []string{address})
	if err != nil {
		t.Fatal(err)
	}

	ingReq, err := ReadIngestRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	if ingReq.ProviderID != peerID {
		t.Fatal("provider ID in request not same as original")
	}
	if !bytes.Equal(ingReq.ContextID, ctxID) {
		t.Fatal("ContextID in request not same as original")
	}
	if !bytes.Equal(ingReq.Metadata, metadata) {
		t.Fatal("metadata in request not same as original")
	}
	if !bytes.Equal([]byte(ingReq.Multihash), []byte(mhs[0])) {
		t.Fatal("multihash in request not same as original")
	}
	if address != ingReq.Addrs[0] {
		t.Fatal("Address in reqest is not same as original")
	}
}
