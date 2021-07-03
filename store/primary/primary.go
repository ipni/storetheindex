package primary

import (
	"sync"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/gammazero/radixtree"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Storage = &rtStorage{}

// wraps a tree with its own mutex
type tree struct {
	lk   sync.RWMutex
	tree *radixtree.Bytes
}

// Adaptive Radix Tree primary storage
type rtStorage struct {
	lk        sync.Mutex
	primary   *tree
	secondary *tree
	sizeLimit int
}

// New creates a new Adaptive Radix Tree storage
func New(size int) store.Storage {
	return &rtStorage{
		primary:   &tree{tree: radixtree.New()},
		secondary: nil,
		sizeLimit: size,
	}
}

// Get info for CID from primary storage.
func (s *rtStorage) Get(c cid.Cid) ([]store.IndexEntry, bool) {
	// Keys indexed as multihash
	k := string(c.Hash())

	// Search primary storage
	s.primary.lk.RLock()
	v, found := s.primary.tree.Get(k)
	s.primary.lk.RUnlock()
	if found {
		return v.([]store.IndexEntry), found
	}

	if s.secondary != nil {
		// Search secondary if not found in the first.
		s.secondary.lk.RLock()
		defer s.secondary.lk.RUnlock()
		v, found = s.secondary.tree.Get(k)
	}

	// If nothing has been found return nil
	if !found {
		return nil, false
	}
	return v.([]store.IndexEntry), found
}

// Put adds indexEntry info for a CID. Put currently is non-distructive
// so if a key for a Cid is already set, we update instead of overwriting
// the value.
// NOTE: We are storing indexEntries in a list, as we don't need to perform
// lookups and we will end up returning all entries. If this assumption
// changes consider using a map.
func (s *rtStorage) Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}
	// Check size of primary storage for eviction purposes.
	s.checkSize()
	// Get multihash from cid
	k := string(c.Hash())
	s.primary.lk.Lock()

	old, found := s.primary.tree.Get(k)
	// If found it means there is already a value there.
	if found {
		// Check if we are trying to put a duplicate entry
		// NOTE: If we end up having a lot of entries for the
		// same CID we may choose to change IndexEntry to a map[peer.ID]pieceID
		// to speed-up this lookup. Don't think is the case right now.
		if !duplicateEntry(in, old.([]store.IndexEntry)) {
			// If not duplicate entry, append to the end
			s.primary.tree.Put(k, append(old.([]store.IndexEntry), in))
		}
	} else {
		s.primary.tree.Put(k, []store.IndexEntry{in})
	}
	s.primary.lk.Unlock()

	// NOTE: Insert in the radix-tree used doesn't return any error.
	// This may change in the future so keeping an error as output in the signature
	// for now.
	return nil
}

// PutMany puts store.IndexEntry information in several CIDs.
// This is usually triggered when a bulk update for a providerID-pieceID
// arrives.
func (s *rtStorage) PutMany(cids []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	for i := range cids {
		err := s.Put(cids[i], provID, pieceID)
		if err != nil {
			return err
		}
	}

	// We are disregarding the CIDs that couldn't be added from the batch
	// for now, so we return no error.
	return nil
}

// Checks if the entry already exists in the index. An entry
// for the same provider but a different piece is not considered
// a duplicate entry (at least for now)
func duplicateEntry(in store.IndexEntry, old []store.IndexEntry) bool {
	for i := range old {
		if in.PieceID == old[i].PieceID &&
			in.ProvID == old[i].ProvID {
			return true
		}
	}
	return false
}

// checksize to see if we need to create a new tree
// If the size is equal or over sizeLimit create new instance
// of the tree
// NOTE: Should we add a security margin? Aim for the 0.45 instead
// of 0.5
func (s *rtStorage) checkSize() {
	// s.fullLock()
	// defer s.fullUnlock()
	s.primary.lk.RLock()
	// TODO: Here we are just looking to the length of the radix tree not the
	// actual storage size. This needs to be changed so we check the actual size being
	// used by the tree.
	if s.primary.tree.Len() >= int(float64(0.5)*float64(s.sizeLimit)) {
		s.lk.Lock()
		// Create new tree and make primary = secondary
		s.secondary = s.primary
		s.primary = &tree{tree: radixtree.New()}
		s.lk.Unlock()
	}
	s.primary.lk.RUnlock()
}

// convenient function to lock the storage and its trees for eviction
func (s *rtStorage) fullLock() {
	s.lk.Lock()
	s.primary.lk.Lock()
	// No need to lock secondary storage. We never write on it
	// s.secondary.lk.Lock()
}

// same with unlcok
func (s *rtStorage) fullUnlock() {
	s.lk.Unlock()
	s.primary.lk.Lock()
}
