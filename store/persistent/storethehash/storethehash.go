package storethehash

import (
	"os"
	"path/filepath"
	"time"

	"github.com/filecoin-project/storetheindex/store"
	cidprimary "github.com/ipld/go-storethehash/store/primary/cid"

	"github.com/ipfs/go-cid"
	sth "github.com/ipld/go-storethehash/store"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Storage = &sthStorage{}

// TODO: Benchmark and fine-tune for better performance.
const DefaultIndexSizeBits = uint8(24)
const DefaultBurstRate = 4 * 1024 * 1024
const DefaultSyncInterval = time.Second

type sthStorage struct {
	dir   string
	store *sth.Store
}

func New(dir string) (store.Storage, error) {
	// NOTE: Using a single file to store index and data.
	// This may change in the future, and we may choose to set
	// a max. size to files. Having several files for storage
	// increases complexity but mimizes the overhead of compaction
	// (once we have it)
	indexPath := filepath.Join(dir, "storethehash.index")
	dataPath := filepath.Join(dir, "storethehash.data")
	primary, err := cidprimary.OpenCIDPrimary(dataPath)
	if err != nil {
		return nil, err
	}

	s, err := sth.OpenStore(indexPath, primary, DefaultIndexSizeBits, DefaultSyncInterval, DefaultBurstRate)
	if err != nil {
		return nil, err
	}
	return &sthStorage{dir: dir, store: s}, nil
}

func (s *sthStorage) Get(c cid.Cid) ([]store.IndexEntry, bool, error) {
	return s.get(c.Bytes())

}

func (s *sthStorage) get(k []byte) ([]store.IndexEntry, bool, error) {
	value, found, err := s.store.Get(k)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	out, err := store.Unmarshal(value)
	if err != nil {
		return nil, false, err
	}
	return out, true, nil

}

func (s *sthStorage) Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}
	return s.put(c.Bytes(), in)
}

func (s *sthStorage) put(k []byte, in store.IndexEntry) error {
	// NOTE: The implementation of Put in storethehash already
	// performs a first lookup to check the type of update that
	// needs to be done over the key. We can probably save this
	// additional get access by implementing the duplicateEntry comparison
	// low-level
	old, found, err := s.get(k)
	if err != nil {
		return err
	}
	// If found it means there is already a value there.
	// Check if we are trying to put a duplicate entry
	if found && store.DuplicateEntry(in, old) {
		return nil
	}

	li := append(old, in)
	b, err := store.Marshal(li)
	if err != nil {
		return err
	}

	return s.store.Put(k, b)
}

func (s *sthStorage) PutMany(cs []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}
	for _, c := range cs {
		err := s.put(c.Bytes(), in)
		if err != nil {
			// TODO: Log error but don't return. Errors for a single
			// CID shouldn't stop from putting the rest.
			continue
		}
	}
	return nil
}

func (s *sthStorage) Close() {
	s.store.Flush()
}

func (s *sthStorage) Size() (int64, error) {
	size := int64(0)
	fi, err := os.Stat(filepath.Join(s.dir, "storethehash.data"))
	if err != nil {
		return size, err
	}
	size += fi.Size()
	fi, err = os.Stat(filepath.Join(s.dir, "storethehash.index"))
	if err != nil {
		return size, err
	}
	size += fi.Size()
	fi, err = os.Stat(filepath.Join(s.dir, "storethehash.index.free"))
	if err != nil {
		return size, err
	}
	size += fi.Size()
	return size, nil

}
