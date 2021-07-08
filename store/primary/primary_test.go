package primary

import (
	"testing"

	"github.com/filecoin-project/storetheindex/utils"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var p peer.ID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"

func TestPutGetRemove(t *testing.T) {
	s := New(1000000)
	cids, err := utils.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	piece := cids[0]
	otherPiece := cids[1]
	single := cids[2]
	noadd := cids[3]
	batch := cids[4:]

	// Put a single CID
	t.Log("Put/Get a single CID in primary storage")
	ok, err := s.Put(single, p, piece)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Did not put new single cid")
	}
	ents, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single cid")
	}
	if ents[0].PieceID != piece || ents[0].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	t.Log("Put existing CID provider-piece entry")
	ok, err = s.Put(single, p, piece)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have put new entry")
	}

	t.Log("Put existing CID and provider with new piece entry")
	ok, err = s.Put(single, p, otherPiece)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should have put new entry")
	}

	t.Log("Check for all entries for single CID")
	ents, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from the batch")
	}
	if len(ents) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if ents[1].PieceID != otherPiece || ents[1].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	// Put a batch of CIDs
	t.Log("Put/Get a batch of CIDs in primary storage")
	count, err := s.PutMany(batch, p, piece)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("Did not put batch of cids")
	}
	t.Logf("Stored %d new entries out of %d total", count, len(batch))

	ents, found, err = s.Get(cids[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("did not find a cid from the batch")
	}
	if ents[0].PieceID != piece || ents[0].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	// Get a key that is not set
	t.Log("Get non-existing key")
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("Error, the key for the cid shouldn't be set")
	}

	t.Log("Remove entry for CID")
	ok, err = s.Remove(single, p, otherPiece)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should have removed entry")
	}

	t.Log("Check for all entries for single CID")
	ents, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from the batch")
	}
	if len(ents) != 1 {
		t.Fatal("Update over existing key not correct")
	}
	if ents[0].PieceID != piece || ents[0].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	t.Log("Remove only entry for CID")
	ok, err = s.Remove(single, p, piece)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should have removed entry")
	}
	_, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("Should not have found CID with no entries")
	}
	t.Log("Remove entry for non-existent CID")
	ok, err = s.Remove(single, p, piece)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should not have removed non-existent entry")
	}

	cidCount := s.CidCount()
	t.Log("Remove provider")
	removed, err := s.RemoveProvider(p)
	if err != nil {
		t.Fatal(err)
	}
	if removed < int64(cidCount) {
		t.Fatalf("should have removed at least %d entries, only removed %d", cidCount, removed)
	}
	if s.CidCount() != 0 {
		t.Fatal("should have 0 size after removing only provider")
	}
}

func TestRotate(t *testing.T) {
	const maxSize = 10

	cids, err := utils.RandomCids(2)
	if err != nil {
		t.Fatal(err)
	}
	piece := cids[0]
	piece2 := cids[1]

	s := New(maxSize * 2)
	cids, err = utils.RandomCids(maxSize + 5)
	if err != nil {
		t.Fatal(err)
	}

	count, err := s.PutMany(cids, p, piece)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("did not put batch of cids")
	}

	_, found, err := s.Get(cids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from previous cache")
	}

	_, found, err = s.Get(cids[maxSize+2])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from new cache")
	}

	cids2, err := utils.RandomCids(maxSize)
	if err != nil {
		t.Fatal(err)
	}

	count, err = s.PutMany(cids2, p, piece2)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("did not put batch of cids")
	}

	// Should find this because it was moved to new cache after 1st rotation
	_, found, err = s.Get(cids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from previous cache")
	}

	// Should find this because it should be in old cache after 2nd rotation
	_, found, err = s.Get(cids[maxSize+2])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from new cache")
	}

	// Should not find this because it was only in old cache after 1st rotation
	_, found, err = s.Get(cids[2])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("cid should have been rotated out of cache")
	}
}
