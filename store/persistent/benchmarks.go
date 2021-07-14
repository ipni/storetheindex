package persistent

import (
	"context"
	"fmt"
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
		entry := store.MakeIndexEntry(p, protocolID, c.Bytes())
		_, err = s.Put(c, entry)
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

func BenchSingleGet(s store.PersistentStorage, size string, b *testing.B) {
	m := initMetrics()
	prepare(s, size, b)
	read(s, size, m, b)
	err := s.Flush()
	if err != nil {
		b.Fatal(err)
	}

	report(s, m, true, b)
}

func BenchParallelGet(s store.PersistentStorage, size string, b *testing.B) {
	var wg sync.WaitGroup

	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			BenchSingleGet(s, size, b)

		}(&wg)
	}
	wg.Wait()
}

func BenchCidGet(s store.PersistentStorage, b *testing.B) {
	cids, err := utils.RandomCids(1)
	if err != nil {
		panic(err)
	}
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")

	entry := store.MakeIndexEntry(p, protocolID, cids[0].Bytes())

	cids, _ = utils.RandomCids(4096)
	s.PutMany(cids, entry)

	for testCount := 1024; testCount < 10240; testCount *= 2 {
		b.Run(fmt.Sprint("Get", testCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			m := initMetrics()
			for i := 0; i < b.N; i++ {
				for j := 0; j < testCount; j++ {
					now := time.Now()
					_, ok, _ := s.Get(cids[j%len(cids)])
					if !ok {
						panic("missing cid")
					}
					m.getTime.add(time.Since(now).Microseconds())
				}
			}
			report(s, m, false, b)
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

	for testCount := 1024; testCount < 10240; testCount *= 2 {
		b.Run(fmt.Sprint("Get", testCount), func(b *testing.B) {
			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup) {
					m := initMetrics()
					for i := 0; i < b.N; i++ {
						for j := 0; j < testCount; j++ {
							now := time.Now()
							_, ok, _ := s.Get(cids[j%len(cids)])
							if !ok {
								panic("missing cid")
							}
							m.getTime.add(time.Since(now).Microseconds())
						}
					}
					report(s, m, false, b)
					wg.Done()
				}(&wg)
			}
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

func report(s store.PersistentStorage, m *metrics, storage bool, b *testing.B) {
	memSize, _ := s.Size()
	avgT := m.getTime.avg() / 1000
	b.Log("Avg time per get (ms):", avgT)
	if storage {
		sizeMB := float64(memSize) / 1000000
		b.Log("Memory size (MB):", sizeMB)
	}
}

func SkipStorage(b *testing.B) {
	if os.Getenv("TEST_STORAGE") == "" {
		b.SkipNow()
	}
}
