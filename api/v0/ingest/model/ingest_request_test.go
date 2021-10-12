package model

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/test/util"
)

func TestIngestRequest(t *testing.T) {
	mhs, err := util.RandomMultihashes(1)
	if err != nil {
		t.Fatal(err)
	}

	metadata := indexer.Metadata{
		Data: []byte("hello"),
	}

	peerID, privKey, err := providerIdent.Decode()
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

	value := indexer.Value{peerID, ctxID, metadata.Encode()}

	if !ingReq.Value.Equal(value) {
		t.Fatal("value in request not same as original")
	}

	if !bytes.Equal([]byte(ingReq.Multihash), []byte(mhs[0])) {
		t.Fatal("multihash in request not same as original")
	}

	if address != ingReq.Addrs[0] {
		t.Fatal("Address in reqest is not same as original")
	}
}
