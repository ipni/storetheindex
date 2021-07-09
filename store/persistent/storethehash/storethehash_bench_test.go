package storethehash_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/importer"
	"github.com/filecoin-project/storetheindex/store"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const testDataDir = "../../test_data/"
const testDataExt = ".data"

func BenchmarkSingle10KB(b *testing.B) {
	benchSingleGet("10KB", b)
}

func BenchmarkSingle10MB(b *testing.B) {
	benchSingleGet("10MB", b)
}

func BenchmarkSingle100MB(b *testing.B) {
	benchSingleGet("100MB", b)
}

func BenchmarkSingle1GB(b *testing.B) {
	benchSingleGet("1GB", b)
}

func BenchmarkParallel10KB(b *testing.B) {
	benchSingleGet("10KB", b)
}

func BenchmarkParallel10MB(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			BenchmarkSingle10MB(b)
		}
	})
}

func BenchmarkParallel100MB(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			BenchmarkSingle100MB(b)
		}
	})
}

func BenchmarkParallel1GB(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			BenchmarkSingle100MB(b)
		}
	})
}

func prepare(s store.Storage, size string, b *testing.B) {
	out := make(chan cid.Cid)
	errOut := make(chan error, 1)

	file, err := os.OpenFile(testDataDir+size+testDataExt, os.O_RDONLY, 0644)
	if err != nil {
		b.Fatalf("couldn't find the right input file for %v, try synthetizing from CLI: %v", size, err)
	}
	defer file.Close()

	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	imp := importer.NewCidListImporter(file)

	go imp.Read(context.Background(), out, errOut)
	for c := range out {
		err = s.Put(c, p, c)
		if err != nil {
			b.Fatal(err)
		}
	}
	err = <-errOut
	if err != nil {
		b.Fatal(err)
	}

}

func read(s store.Storage, size string, m *metrics, b *testing.B) {
	out := make(chan cid.Cid)
	errOut := make(chan error, 1)

	file, err := os.OpenFile(testDataDir+size+testDataExt, os.O_RDONLY, 0644)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	imp := importer.NewCidListImporter(file)

	b.ResetTimer()
	go imp.Read(context.Background(), out, errOut)
	for c := range out {
		now := time.Now()
		_, found, err := s.Get(c)
		if err != nil || !found {
			b.Errorf("cid not found")
		}
		m.getTime.add(time.Since(now).Microseconds())

	}
	err = <-errOut
	if err != nil {
		b.Fatal(err)
	}

}

func benchSingleGet(size string, b *testing.B) {
	// Init storage
	s, err := initSth()
	if err != nil {
		b.Fatal(err)
	}
	m := initMetrics()
	prepare(s, size, b)
	read(s, size, m, b)
	err = s.Flush()
	if err != nil {
		b.Fatal(err)
	}

	memSize, _ := s.Size()
	b.ReportMetric(float64(memSize)/1000000, "MB")
	b.ReportMetric(m.getTime.avg()/1000, "ms/op")
}

type metric struct {
	val int64
	n   uint64
}

func (m *metric) add(val int64) {
	m.val += val
	m.n++
}

func (m *metric) avg() float64 {
	return float64(m.val) / float64(m.n)
}

type metrics struct {
	getTime *metric
}

func initMetrics() *metrics {
	return &metrics{
		getTime: &metric{},
	}
}
