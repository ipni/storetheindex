package model

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

func TestMarshal(t *testing.T) {
	syncCid, err := cid.Decode("baguqeeqqeldcdzz3zngyencjoo7h5i5wti")
	if err != nil {
		panic(err)
	}

	peerAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		panic(err)
	}

	// Masrhal request and check e2e
	req := &SyncRequest{
		SyncCid:  syncCid,
		PeerAddr: peerAddr,
	}

	b, err := MarshalSyncRequest(req)
	if err != nil {
		t.Fatalf("Failed to marshal SyncRequest: %s", err)
	}

	t.Log("Req1:", string(b))

	r, err := UnmarshalSyncRequest(b)
	if err != nil {
		t.Fatalf("Failed to unmarshal SyncRequest: %s", err)
	}

	if r.SyncCid != req.SyncCid {
		t.Fatal("cid changed by serialization")
	}

	if r.PeerAddr.String() != req.PeerAddr.String() {
		t.Fatal("addr changed by serialization:", r.PeerAddr)
	}
}
