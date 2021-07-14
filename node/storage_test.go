package node

import (
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent/storethehash"
	"github.com/filecoin-project/storetheindex/store/primary"
	"github.com/filecoin-project/storetheindex/utils"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const protocolID = 0

func initStorage(t *testing.T, withPrimary bool, withPersist bool) *nodeStorage {
	tmpDir := t.TempDir()
	var prim store.Storage
	var persistent store.PersistentStorage
	var err error

	if withPersist {
		persistent, err = storethehash.New(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
	}
	if withPrimary {
		prim = primary.New(100000)
	}
	return NewStorage(prim, persistent)
}

func TestPassthrough(t *testing.T) {
	s := initStorage(t, true, true)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := utils.RandomCids(5)
	if err != nil {
		t.Fatal(err)
	}

	entry1 := store.MakeIndexEntry(p, protocolID, cids[0].Bytes())
	entry2 := store.MakeIndexEntry(p, protocolID, cids[1].Bytes())
	single := cids[2]

	// First put should go to persistent
	_, err = s.Put(single, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	_, persf, _ := s.persistent.Get(single)
	_, primf, _ := s.primary.Get(single)
	if !persf || primf {
		t.Fatal("single put went to persistent and cache")
	}

	// Getting the value should put it in cache
	v, found, _ := s.Get(single)
	if !found || !v[0].Equal(entry1) {
		t.Fatal("value not found in combined storage")
	}
	_, primf, _ = s.primary.Get(single)
	if !primf {
		t.Fatal("cid not moved to cache after miss get")
	}

	// Updating an existing CID should also update cache
	_, err = s.Put(single, entry2)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	persv, _, _ := s.persistent.Get(single)
	primv, _, _ := s.primary.Get(single)
	if len(primv) != 2 || len(persv) != 2 {
		t.Fatal("value not updated in cache and persistent")
	}

	// Remove should apply to both storages
	_, err = s.Remove(single, entry1)
	if err != nil {
		t.Fatal(err)
	}
	persv, _, _ = s.persistent.Get(single)
	primv, _, _ = s.primary.Get(single)
	if len(primv) != 1 || len(persv) != 1 {
		t.Fatal("entry not removed in both storages")
	}

	// Putting many should only update in cache the ones
	// already stored, adding all to persistent storage.
	err = s.PutMany(cids[2:], entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	persv, _, _ = s.persistent.Get(single)
	primv, _, _ = s.primary.Get(single)
	if len(primv) != 2 || len(persv) != 2 {
		t.Fatal("value not updated in cache and persistent after PutMany")
	}
	// This CID should only be found in persistent
	_, persf, _ = s.persistent.Get(cids[4])
	_, primf, _ = s.primary.Get(cids[4])
	if !persf || primf {
		t.Fatal("single put went to persistent and cache")
	}

	// RemoveMany should remove the corresponding from both storages.
	err = s.RemoveMany(cids[2:], entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}
	persv, _, _ = s.persistent.Get(single)
	primv, _, _ = s.primary.Get(single)
	if len(primv) != 1 || len(persv) != 1 {
		t.Fatal("value not removed in cache and persistent after RemoveMany")
	}
	_, persf, _ = s.persistent.Get(cids[4])
	_, primf, _ = s.primary.Get(cids[4])
	if persf || primf {
		t.Fatal("remove many didn't remove from both storages")
	}

}

func TestOnlyPrimary(t *testing.T) {
	s := initStorage(t, true, false)
	e2e(t, s)
}

func TestOnlyPersistent(t *testing.T) {
	s := initStorage(t, false, true)
	e2e(t, s)
}

func TestBoth(t *testing.T) {
	s := initStorage(t, true, true)
	e2e(t, s)
}

func e2e(t *testing.T, s *nodeStorage) {
	// Create new valid peer.ID
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := utils.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	entry1 := store.MakeIndexEntry(p, protocolID, cids[0].Bytes())
	entry2 := store.MakeIndexEntry(p, protocolID, cids[1].Bytes())

	single := cids[2]
	noadd := cids[3]
	batch := cids[4:]
	remove := cids[4]

	// Put a single CID
	t.Logf("Put/Get a single CID in storage")
	_, err = s.Put(single, entry1)
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
	if !i[0].Equal(entry1) {
		t.Errorf("Got wrong value for single cid")
	}

	// Put a batch of CIDs
	t.Logf("Put/Get a batch of CIDs in storage")
	err = s.PutMany(batch, entry1)
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
	if !i[0].Equal(entry1) {
		t.Errorf("Got wrong value for single cid")
	}

	// Put on an existing key
	t.Logf("Put/Get on existing key")
	_, err = s.Put(single, entry2)
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
	if !i[1].Equal(entry2) {
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
	_, err = s.Remove(remove, entry1)
	if err != nil {
		t.Fatal("Error putting single cid: ", err)
	}

	_, found, err = s.Get(remove)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Errorf("cid should have been removed")
	}

	// Remove an entry from the key
	_, err = s.Remove(single, entry1)
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

func SizeTest(t *testing.T) {
	s := initStorage(t, true, true)
	// Init storage
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	cids, err := utils.RandomCids(151)
	if err != nil {
		t.Fatal(err)
	}

	entry := store.MakeIndexEntry(p, protocolID, cids[0].Bytes())
	for _, c := range cids[1:] {
		_, err = s.Put(c, entry)
		if err != nil {
			t.Fatal(err)
		}
	}

	size, err := s.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == int64(0) {
		t.Error("failed to compute storage size")
	}
	s = initStorage(t, true, false)
	_, err = s.Size()
	if err == nil {
		t.Fatal("should return an error when no persistence configured")
	}
}
