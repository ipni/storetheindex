package storethehash_test

import (
	"io/ioutil"
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent"
	"github.com/filecoin-project/storetheindex/store/persistent/storethehash"
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
