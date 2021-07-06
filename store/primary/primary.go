package primary

import (
	"sync"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/gammazero/radixtree"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// numLocks is the lock granularity for radixtree. Must be power of two.
const numLocks = 64

var _ store.Storage = &rtStorage{}

// Adaptive Radix Tree primary storage
type rtStorage struct {
	branchLocks []sync.Mutex
	current     *radixtree.Bytes
	previous    *radixtree.Bytes
	sizeLimit   int
}

// New creates a new Adaptive Radix Tree storage
func New(size int) store.Storage {
	return &rtStorage{
		branchLocks: make([]sync.Mutex, numLocks),
		current:     radixtree.New(),
		previous:    nil,
		sizeLimit:   size / 2,
	}
}

// Get info for CID from primary storage.
func (s *rtStorage) Get(c cid.Cid) ([]store.IndexEntry, bool) {
	// Keys indexed as multihash
	k := string(c.Hash())

	s.lockBranch(k)
	defer s.unlockBranch(k)

	return s.get(k)
}

func (s *rtStorage) get(k string) ([]store.IndexEntry, bool) {
	// Search current cache
	v, found := s.current.Get(k)
	if found {
		return v.([]store.IndexEntry), found
	}

	if s.previous == nil {
		return nil, false
	}

	// Search previous if not found in current
	v, found = s.previous.Get(k)

	// If nothing has been found return nil
	if !found {
		return nil, false
	}

	// Put the value found in the previous tree into the current one.
	s.current.Put(k, v)

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

	// Get multihash from cid
	k := string(c.Hash())

	s.lockBranch(k)

	if !s.put(k, in) {
		s.unlockBranch(k)
		return nil
	}

	if s.current.Len() >= s.sizeLimit {
		s.unlockBranch(k)
		s.rotateCache()
	} else {
		s.unlockBranch(k)
	}

	return nil
}

func (s *rtStorage) put(k string, entry store.IndexEntry) bool {
	// Get from current or previous cache
	old, found := s.get(k)
	// If found it means there is already a value there.
	// Check if we are trying to put a duplicate entry
	// NOTE: If we end up having a lot of entries for the
	// same CID we may choose to change IndexEntry to a map[peer.ID]pieceID
	// to speed-up this lookup. Don't think is the case right now.
	if found && duplicateEntry(entry, old) {
		return false
	}

	s.current.Put(k, append(old, entry))
	return true
}

func (s *rtStorage) rotateCache() {
	// Acquire all locks
	for i := range s.branchLocks {
		s.branchLocks[i].Lock()
	}

	if s.current.Len() >= s.sizeLimit {
		s.previous, s.current = s.current, radixtree.New()
	}

	// Release all locks
	for i := range s.branchLocks {
		s.branchLocks[i].Unlock()
	}
}

// PutMany puts store.IndexEntry information in several CIDs.
// This is usually triggered when a bulk update for a providerID-pieceID
// arrives.
func (s *rtStorage) PutMany(cids []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}
	var addedEntry bool

	for i := range cids {
		k := string(cids[i].Hash())
		s.lockBranch(k)

		if s.put(k, in) {
			addedEntry = true
		}
		s.unlockBranch(k)
	}

	if addedEntry {
		// Check if rotation needed
		s.lockBranch("")
		if s.current.Len() >= s.sizeLimit {
			s.unlockBranch("")
			s.rotateCache()
		} else {
			s.unlockBranch("")
		}
	}

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

func (s *rtStorage) lockBranch(k string) {
	var idx int
	if k != "" {
		// bitwise modulus requires that s.locks is power of 2
		idx = int(k[0]) & (len(s.branchLocks) - 1)
	}
	s.branchLocks[idx].Lock()
}

func (s *rtStorage) unlockBranch(k string) {
	var idx int
	if k != "" {
		// bitwise modulus requires that s.locks is power of 2
		idx = int(k[0]) & (len(s.branchLocks) - 1)
	}
	s.branchLocks[idx].Unlock()
}
