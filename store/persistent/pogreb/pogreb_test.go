package pogreb_test

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent"
	"github.com/filecoin-project/storetheindex/store/persistent/pogreb"
)

func initPogreb() (store.StorageFlusher, error) {
	tmpDir, err := ioutil.TempDir("", "sth")
	if err != nil {
		return nil, err
	}
	return pogreb.New(tmpDir)
}

func TestE2E(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	persistent.E2ETest(t, s)
}

func TestSize(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	persistent.SizeTest(t, s)
}

func TestRemoveMany(t *testing.T) {
	skipIf32bit(t)

	s, err := initPogreb()
	if err != nil {
		t.Fatal(err)
	}
	persistent.RemoveManyTest(t, s)
}

func skipIf32bit(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skip("Pogreb cannot use GOARCH=386")
	}
}
