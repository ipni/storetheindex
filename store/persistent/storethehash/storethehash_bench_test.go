package storethehash_test

import (
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent"
)

func initBenchStore() store.PersistentStorage {
	s, err := initSth()
	if err != nil {
		panic(err)
	}
	return s
}

func BenchmarkGet(b *testing.B) {
	persistent.BenchCidGet(initBenchStore(), b)
}
func BenchmarkParallelGet(b *testing.B) {
	persistent.BenchParallelCidGet(initBenchStore(), b)
}

// To run this storage benchmarks run:
// TEST_STORAGE=true go test -v -timeout=30m
func TestBenchSingle10MB(t *testing.T) {
	persistent.SkipStorage(t)
	persistent.BenchReadAll(initBenchStore(), "10MB", t)
}

func TestBenchSingle100MB(t *testing.T) {
	persistent.SkipStorage(t)
	persistent.BenchReadAll(initBenchStore(), "100MB", t)
}

func TestBenchSingle1GB(t *testing.T) {
	persistent.SkipStorage(t)
	persistent.BenchReadAll(initBenchStore(), "1GB", t)
}
