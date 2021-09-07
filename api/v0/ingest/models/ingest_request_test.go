package models

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestIngestRequest(t *testing.T) {
	cids, err := utils.RandomCids(1)
	if err != nil {
		t.Fatal(err)
	}

	metadata := []byte("hello")

	data, err := MakeIngestRequest(providerIdent, cids[0], 0, metadata)
	if err != nil {
		t.Fatal(err)
	}

	ingReq, err := ReadIngestRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	peerID, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		t.Fatal(err)
	}

	value := indexer.MakeValue(peerID, 0, metadata)

	if !ingReq.Value.Equal(value) {
		t.Fatal("value in request not same as original")
	}

	if !ingReq.Cid.Equals(cids[0]) {
		t.Fatal("cid in request not same as original")
	}
}
