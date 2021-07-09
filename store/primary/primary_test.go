package primary

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/filecoin-project/storetheindex/utils"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var p peer.ID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"

func TestPutGetRemove(t *testing.T) {
	s := New(1000000)
	cids, err := utils.RandomCids(15)
	if err != nil {
		t.Fatal(err)
	}

	piece := cids[0]
	otherPiece := cids[1]
	single := cids[2]
	noadd := cids[3]
	batch := cids[4:]

	// Put a single CID
	t.Log("Put/Get a single CID in primary storage")
	if !s.PutCheck(single, p, piece) {
		t.Fatal("Did not put new single cid")
	}
	ents, found, err := s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding single cid")
	}
	if ents[0].PieceID != piece || ents[0].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	t.Log("Put existing CID provider-piece entry")
	if s.PutCheck(single, p, piece) {
		t.Fatal("should not have put new entry")
	}

	t.Log("Put existing CID and provider with new piece entry")
	if !s.PutCheck(single, p, otherPiece) {
		t.Fatal("should have put new entry")
	}

	t.Log("Check for all entries for single CID")
	ents, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from the batch")
	}
	if len(ents) != 2 {
		t.Fatal("Update over existing key not correct")
	}
	if ents[1].PieceID != otherPiece || ents[1].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	// Put a batch of CIDs
	t.Log("Put/Get a batch of CIDs in primary storage")
	count := s.PutManyCount(batch, p, piece)
	if count == 0 {
		t.Fatal("Did not put batch of cids")
	}
	t.Logf("Stored %d new entries out of %d total", count, len(batch))

	ents, found, err = s.Get(cids[5])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("did not find a cid from the batch")
	}
	if ents[0].PieceID != piece || ents[0].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	// Get a key that is not set
	t.Log("Get non-existing key")
	_, found, err = s.Get(noadd)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("Error, the key for the cid shouldn't be set")
	}

	t.Log("Remove entry for CID")
	if !s.RemoveCheck(single, p, otherPiece) {
		t.Fatal("should have removed entry")
	}

	t.Log("Check for all entries for single CID")
	ents, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from the batch")
	}
	if len(ents) != 1 {
		t.Fatal("Update over existing key not correct")
	}
	if ents[0].PieceID != piece || ents[0].ProvID != p {
		t.Error("Got wrong value for single cid")
	}

	t.Log("Remove only entry for CID")
	if !s.RemoveCheck(single, p, piece) {
		t.Fatal("should have removed entry")
	}
	_, found, err = s.Get(single)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("Should not have found CID with no entries")
	}
	t.Log("Remove entry for non-existent CID")
	if s.RemoveCheck(single, p, piece) {
		t.Fatal("should not have removed non-existent entry")
	}

	cidCount := s.CidCount()
	t.Log("Remove provider")
	removed := s.RemoveProviderCount(p)
	if removed < cidCount {
		t.Fatalf("should have removed at least %d entries, only removed %d", cidCount, removed)
	}
	if s.CidCount() != 0 {
		t.Fatal("should have 0 size after removing only provider")
	}
}

func TestRotate(t *testing.T) {
	const maxSize = 10

	cids, err := utils.RandomCids(2)
	if err != nil {
		t.Fatal(err)
	}
	piece := cids[0]
	piece2 := cids[1]

	s := New(maxSize * 2)
	cids, err = utils.RandomCids(maxSize + 5)
	if err != nil {
		t.Fatal(err)
	}

	if s.PutManyCount(cids, p, piece) == 0 {
		t.Fatal("did not put batch of cids")
	}

	_, found, err := s.Get(cids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from previous cache")
	}

	_, found, err = s.Get(cids[maxSize+2])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from new cache")
	}

	cids2, err := utils.RandomCids(maxSize)
	if err != nil {
		t.Fatal(err)
	}

	if s.PutManyCount(cids2, p, piece2) == 0 {
		t.Fatal("did not put batch of cids")
	}

	// Should find this because it was moved to new cache after 1st rotation
	_, found, err = s.Get(cids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from previous cache")
	}

	// Should find this because it should be in old cache after 2nd rotation
	_, found, err = s.Get(cids[maxSize+2])
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("Error finding a cid from new cache")
	}

	// Should not find this because it was only in old cache after 1st rotation
	_, found, err = s.Get(cids[2])
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("cid should have been rotated out of cache")
	}
}

func TestMem1024K(t *testing.T) {
	skipUnlessMemUse(t)

	cids, err := utils.RandomCids(1)
	if err != nil {
		panic(err)
	}
	piece := cids[0]

	s := New(1024 * 1064)
	for i := 0; i < 1024; i++ {
		cids, _ = utils.RandomCids(1024)
		s.PutManyCount(cids, p, piece)
	}

	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	t.Log("Alloc before GC:", m.Alloc)
	runtime.GC()
	runtime.ReadMemStats(&m)
	t.Log("Alloc after GC: ", m.Alloc)
	t.Log("Items in cache:", s.CidCount())
	t.Log("Rotations:", s.RotationCount())
}

func skipUnlessMemUse(t *testing.T) {
	if os.Getenv("TEST_MEM_USE") == "" {
		t.SkipNow()
	}
}

func BenchmarkPut(b *testing.B) {
	cids, err := utils.RandomCids(1)
	if err != nil {
		panic(err)
	}
	piece := cids[0]

	cids, _ = utils.RandomCids(10240)

	b.Run("Put single", func(b *testing.B) {
		s := New(8192)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = s.Put(cids[i%len(cids)], p, piece)
			if err != nil {
				panic(err)
			}
		}
	})

	for testCount := 1024; testCount < len(cids); testCount *= 2 {
		b.Run(fmt.Sprint("Put", testCount), func(b *testing.B) {
			s := New(8192)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < testCount; j++ {
					err = s.Put(cids[j], p, piece)
					if err != nil {
						panic(err)
					}
				}
			}
		})

		b.Run(fmt.Sprint("PutMany", testCount), func(b *testing.B) {
			s := New(8192)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err = s.PutMany(cids[:testCount], p, piece)
				if err != nil {
					panic(err)
				}
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	cids, err := utils.RandomCids(1)
	if err != nil {
		panic(err)
	}
	piece := cids[0]

	s := New(8192)
	cids, _ = utils.RandomCids(4096)
	s.PutMany(cids, p, piece)

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

	for testCount := 1024; testCount < 10240; testCount *= 2 {
		b.Run(fmt.Sprint("Get", testCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < testCount; j++ {
					_, ok, _ := s.Get(cids[j%len(cids)])
					if !ok {
						panic("missing cid")
					}
				}
			}
		})
	}
}
