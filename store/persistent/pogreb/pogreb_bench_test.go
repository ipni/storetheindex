package pogreb_test

import (
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent"
)

func initBenchStore(b *testing.B) store.StorageFlusher {
	s, err := initPogreb()
	if err != nil {
		b.Fatal(err)
	}
	return s
}
func BenchmarkSingle10KB(b *testing.B) {
	persistent.BenchSingleGet(initBenchStore(b), "10KB", b)
}

func BenchmarkSingle10MB(b *testing.B) {
	persistent.BenchSingleGet(initBenchStore(b), "10MB", b)
}

func BenchmarkSingle100MB(b *testing.B) {
	persistent.BenchSingleGet(initBenchStore(b), "100MB", b)
}

func BenchmarkSingle1GB(b *testing.B) {
	persistent.BenchSingleGet(initBenchStore(b), "1GB", b)
}

func BenchmarkParallel10KB(b *testing.B) {
	persistent.BenchParallelGet(initBenchStore(b), "10KB", b)
}

func BenchmarkParallel10MB(b *testing.B) {
	persistent.BenchParallelGet(initBenchStore(b), "10MB", b)
}

func BenchmarkParallel100MB(b *testing.B) {
	persistent.BenchParallelGet(initBenchStore(b), "100MB", b)
}

func BenchmarkParallel1GB(b *testing.B) {
	persistent.BenchParallelGet(initBenchStore(b), "1GB", b)
}
