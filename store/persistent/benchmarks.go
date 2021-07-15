package persistent

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/importer"
	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/utils"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

const (
	testDataDir = "../../test_data/"
	testDataExt = ".data"
	// protocol ID for IndexEntry metadata
	protocolID = 0
)

// prepare reads a cidlist and imports it to persistent storage getting
// it ready for benchmarking.
func prepare(s store.Storage, size string, t *testing.T) {
	out := make(chan cid.Cid)
	errOut := make(chan error, 1)

	file, err := os.OpenFile(testDataDir+size+testDataExt, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("couldn't find the right input file for %v, try synthetizing from CLI: %v", size, err)
	}
	defer file.Close()

	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	imp := importer.NewCidListImporter(file)

	go imp.Read(context.Background(), out, errOut)

	for c := range out {
		entry := store.MakeIndexEntry(p, protocolID, c.Bytes())
		_, err = s.Put(c, entry)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = <-errOut
	if err != nil {
		t.Fatal(err)
	}

}

// readAll reads all of the cids from a file and tries to get it from
// the persistent storage.
func readAll(s store.Storage, size string, m *metrics, t *testing.T) {
	out := make(chan cid.Cid)
	errOut := make(chan error, 1)

	file, err := os.OpenFile(testDataDir+size+testDataExt, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	imp := importer.NewCidListImporter(file)

	go imp.Read(context.Background(), out, errOut)
	for c := range out {
		now := time.Now()
		_, found, err := s.Get(c)
		if err != nil || !found {
			t.Errorf("cid not found")
		}
		m.getTime.add(time.Since(now).Microseconds())

	}
	err = <-errOut
	if err != nil {
		t.Fatal(err)
	}

}

// Benchmark the average time per get by all CIDs and the total storage used.
func BenchReadAll(s store.PersistentStorage, size string, t *testing.T) {
	m := initMetrics()
	prepare(s, size, t)
	readAll(s, size, m, t)
	err := s.Flush()
	if err != nil {
		t.Fatal(err)
	}

	report(s, m, true, t)
}

// Benchmark single thread get operation
func BenchCidGet(s store.PersistentStorage, b *testing.B) {
	cids, err := utils.RandomCids(1)
	if err != nil {
		panic(err)
	}
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")

	entry := store.MakeIndexEntry(p, protocolID, cids[0].Bytes())

	cids, _ = utils.RandomCids(4096)
	s.PutMany(cids, entry)

	// Bench average time for a single get
	b.Run("Get single", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, ok, _ := s.Get(cids[i%len(cids)])
			if !ok {
				panic("missing cid")
			}
		}
	})

	// Test time to fetch certain amount of requests
	for testCount := 1024; testCount < 10240; testCount *= 2 {
		b.Run(fmt.Sprint("Get", testCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N*testCount; i++ {
				_, ok, _ := s.Get(cids[i%len(cids)])
				if !ok {
					panic("missing cid")
				}
			}
		})
	}
}

func BenchParallelCidGet(s store.PersistentStorage, b *testing.B) {
	cids, err := utils.RandomCids(1)
	if err != nil {
		panic(err)
	}
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")

	entry := store.MakeIndexEntry(p, protocolID, cids[0].Bytes())

	cids, _ = utils.RandomCids(4096)
	s.PutMany(cids, entry)
	rand.Seed(time.Now().UnixNano())

	// Benchmark the average request time for different number of go routines.
	for rout := 10; rout <= 100; rout += 10 {
		b.Run(fmt.Sprint("Get parallel", rout), func(b *testing.B) {
			var wg sync.WaitGroup
			ch := make(chan bool)
			for i := 0; i < rout; i++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup, ch chan bool) {
					// Each go routine starts fetching CIDs from different offset.
					// TODO: Request follow a zipf distribution.
					off := rand.Int()
					// Wait for all routines to be started
					<-ch
					for i := 0; i < b.N; i++ {
						_, ok, _ := s.Get(cids[(off+i)%len(cids)])
						if !ok {
							panic("missing cid")
						}

					}
					wg.Done()
				}(&wg, ch)
			}
			b.ReportAllocs()
			b.ResetTimer()
			close(ch)
			wg.Wait()
		})
	}
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

func report(s store.PersistentStorage, m *metrics, storage bool, t *testing.T) {
	memSize, _ := s.Size()
	avgT := m.getTime.avg() / 1000
	t.Log("Avg time per get (ms):", avgT)
	if storage {
		sizeMB := float64(memSize) / 1000000
		t.Log("Memory size (MB):", sizeMB)
	}
}

func SkipStorage(t *testing.T) {
	if os.Getenv("TEST_STORAGE") == "" {
		t.SkipNow()
	}
}
