package persistent

import (
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/utils"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func E2ETest(t *testing.T, s store.StorageFlusher) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := utils.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	piece := cids[0]
	single := cids[1]
	noadd := cids[2]
	batch := cids[3:]
	remove := cids[3]

	// Put a single CID
	t.Logf("Put/Get a single CID in primary storage")
	err = s.Put(single, p, piece)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	i, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding single cid")
	}
	if i[0].PieceID != piece || i[0].ProvID != p {
		t.Errorf("Got wrong value for single cid")
	}

	// Put a batch of CIDs
	t.Logf("Put/Get a batch of CIDs in primary storage")
	err = s.PutMany(batch, p, piece)
	if err != nil {
		t.Fatal("Error putting batch of cids: ", err)
	}

	i, found, err = s.Get(cids[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding a cid from the batch")
	}
	if i[0].PieceID != piece || i[0].ProvID != p {
		t.Errorf("Got wrong value for single cid")
	}

	// Put on an existing key
	t.Logf("Put/Get on existing key")
	err = s.Put(single, p, noadd)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	i, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("Error finding a cid from the batch")
	}
	if len(i) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if i[1].PieceID != noadd || i[1].ProvID != p {
		t.Errorf("Got wrong value for single cid")
	}

	// Get a key that is not set
	t.Logf("Get non-existing key")
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("Error, the key for the cid shouldn't be set")
	}

	// Remove a key
	t.Logf("Remove key")
	err = s.Remove(remove, p, piece)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	i, found, err = s.Get(remove)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("cid should have been removed")
	}

	// Remove an entry from the key
	err = s.Remove(single, p, piece)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	i, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("cid should still have one entry")
	}
	if len(i) != 1 {
		t.Errorf("wrong number of entries after remove")
	}

}

func SizeTest(t *testing.T, s store.StorageFlusher) {
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := utils.RandomCids(150)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range cids {
		s.Put(c, p, c)
	}

	size, err := s.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == int64(0) {
		t.Error("failed to compute storage size")
	}
}

func RemoveManyTest(t *testing.T, s store.StorageFlusher) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := utils.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	piece := cids[0]
	batch := cids[1:]

	// Put a batch of CIDs
	t.Logf("Put/Get a batch of CIDs in primary storage")
	err = s.PutMany(batch, p, piece)
	if err != nil {
		t.Fatal("Error putting batch of cids: ", err)
	}

	// Put a single CID
	t.Logf("Remove key")
	err = s.RemoveMany(cids[2:], p, piece)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	i, found, err := s.Get(cids[1])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Errorf("cid should not have been removed")
	}
	if len(i) != 1 {
		t.Errorf("wrong number of cids removed")
	}

}
