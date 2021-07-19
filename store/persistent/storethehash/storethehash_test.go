package storethehash_test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent"
	"github.com/filecoin-project/storetheindex/store/persistent/storethehash"
	"github.com/filecoin-project/storetheindex/utils"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func initSth() (store.PersistentStorage, error) {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		return nil, err
	}
	return storethehash.New(tmpDir)
}

func TestE2E(t *testing.T) {
	s, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	persistent.E2ETest(t, s)
}

func TestSize(t *testing.T) {
	s, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	persistent.SizeTest(t, s)
}

func TestRemoveMany(t *testing.T) {
	s, err := initSth()
	if err != nil {
		t.Fatal(err)
	}
	persistent.RemoveManyTest(t, s)
}

func TestPeriodicFlush(t *testing.T) {
	// Init storage
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		t.Fatal(err)
	}

	s, err := storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}

	// Put some data in the first storage.
	cids, err := utils.RandomCids(151)
	if err != nil {
		t.Fatal(err)
	}

	entry := store.MakeIndexEntry(p, 0, cids[0].Bytes())
	for _, c := range cids[1:] {
		_, err = s.Put(c, entry)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Sleep for 2 sync Intervals to ensure that data is flushed
	time.Sleep(2 * storethehash.DefaultSyncInterval)
	s.Close()

	// Regenerate new storage from primary
	s2, err := storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Get data. If re-generated correctly we should find the CID
	i, found, err := s2.Get(cids[3])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Error finding single cid")
	}
	if !i[0].Equal(entry) {
		t.Errorf("Got wrong value for single cid")
	}

}
