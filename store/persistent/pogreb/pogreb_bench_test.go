package pogreb_test

import (
	"runtime"
	"testing"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent"
)

func initBenchStore(b *testing.B) store.PersistentStorage {
	s, err := initPogreb()
	if err != nil {
		b.Fatal(err)
	}
	return s
}

func BenchmarkSingle10KB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchSingleGet(initBenchStore(b), "10KB", b)
}

func BenchmarkSingle10MB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchSingleGet(initBenchStore(b), "10MB", b)
}

func BenchmarkSingle100MB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchSingleGet(initBenchStore(b), "100MB", b)
}

func BenchmarkSingle1GB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchSingleGet(initBenchStore(b), "1GB", b)
}

func BenchmarkParallel10KB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchParallelGet(initBenchStore(b), "10KB", b)
}

func BenchmarkParallel10MB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchParallelGet(initBenchStore(b), "10MB", b)
}

func BenchmarkParallel100MB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchParallelGet(initBenchStore(b), "100MB", b)
}

func BenchmarkParallel1GB(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.SkipStorage(b)
	persistent.BenchParallelGet(initBenchStore(b), "1GB", b)
}

func BenchmarkGet(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.BenchCidGet(initBenchStore(b), b)
}

func BenchmarkParallelGet(b *testing.B) {
	skipBenchIf32bit(b)
	persistent.BenchParallelCidGet(initBenchStore(b), b)
}

func skipBenchIf32bit(b *testing.B) {
	if runtime.GOARCH == "386" {
		b.Skip("Pogreb cannot use GOARCH=386")
	}
}
