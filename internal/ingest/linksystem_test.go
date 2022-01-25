package ingest

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
)

func TestCidToAdMapping(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewMapDatastore()

	adCid1, err := cid.Decode("baguqeeqq7oiy5nts7qzhefwhpokkzxmzaq")
	if err != nil {
		t.Fatal(err)
	}
	adCid2, err := cid.Decode("baguqeeqqhtvkwigrelprmtqqqrdy32pqc4")
	if err != nil {
		t.Fatal(err)
	}
	entCid, err := cid.Decode("baguqeeqqf2zgykyxbm4e3bmdudylddza4y")
	if err != nil {
		t.Fatal(err)
	}

	// Test writing entry CID with multiple ad CIDs.
	err = pushCidToAdMapping(ctx, ds, entCid, adCid1)
	if err != nil {
		t.Fatal(err)
	}
	err = pushCidToAdMapping(ctx, ds, entCid, adCid2)
	if err != nil {
		t.Fatal(err)
	}

	// Test reading back each ad CID for the entry CID.
	adCid, err := popCidToAdMapping(ctx, ds, entCid)
	if err != nil {
		t.Fatal(err)
	}
	if adCid != adCid2 {
		t.Fatal("wrong ad cid returned from first get")
	}
	adCid, err = popCidToAdMapping(ctx, ds, entCid)
	if err != nil {
		t.Fatal(err)
	}
	if adCid != adCid1 {
		t.Fatal("wrong ad cid returned from second get")
	}

	// Test that entry CID to ad CID is deleted after last read.
	_, err = popCidToAdMapping(ctx, ds, entCid)
	if err == nil {
		t.Fatal("expected error for deleted mapping")
	}
}
