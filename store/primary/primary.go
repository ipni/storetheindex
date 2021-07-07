package primary

import (
	"sync"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/gammazero/radixtree"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// concurrrency is the lock granularity for radixtree. Must be power of two.
const concurrency = 16

var _ store.Storage = &rtStorage{}

// syncCache is a rotatable cache. Multiples instances may be used to decrease
// concurrent access collision.
type syncCache struct {
	current   *radixtree.Bytes
	previous  *radixtree.Bytes
	mutex     sync.Mutex
	rotations int
}

// Adaptive Radix Tree primary storage
type rtStorage struct {
	cacheSet   []*syncCache
	rotateSize int
}

// New creates a new Adaptive Radix Tree storage
func New(size int) store.Storage {
	cacheSetSize := concurrency
	if size < 256 {
		cacheSetSize = 1
	}
	cacheSet := make([]*syncCache, cacheSetSize)
	for i := range cacheSet {
		cacheSet[i] = &syncCache{
			current: radixtree.New(),
		}
	}
	return &rtStorage{
		cacheSet:   cacheSet,
		rotateSize: size / (cacheSetSize * 2),
	}
}

// Get info for CID from primary storage.
func (s *rtStorage) Get(c cid.Cid) ([]store.IndexEntry, bool) {
	// Keys indexed as multihash
	k := string(c.Hash())

	cache := s.getCache(k)
	cache.lock()
	defer cache.unlock()

	return cache.get(k)
}

// Put adds indexEntry info for a CID. Put currently is non-destructive
// so if a key for a Cid is already set, we update instead of overwriting
// the value.
// NOTE: We are storing indexEntries in a list, as we don't need to perform
// lookups and we will end up returning all entries. If this assumption
// changes consider using a map.
func (s *rtStorage) Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}

	// Get multihash from cid
	k := string(c.Hash())
	cache := s.getCache(k)

	cache.lock()
	if cache.put(k, in) && cache.current.Len() > s.rotateSize {
		// Only rotate one cache at a time. This may leave older entries in
		// other caches, but if CIDs are dirstributed evenly over the cache set
		// then over time all members should be rotated the same amount on
		// average.  This is done so that it is not necessary to lock all
		// caches in order to perform a rotation.  This also means that items
		// age out more incrementally.
		cache.rotate()
	}
	cache.unlock()

	return nil
}

// PutMany puts store.IndexEntry information in several CIDs.
// This is usually triggered when a bulk update for a providerID-pieceID
// arrives.
func (s *rtStorage) PutMany(cids []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}

	for i := range cids {
		k := string(cids[i].Hash())
		cache := s.getCache(k)
		cache.lock()
		if cache.put(k, in) && cache.current.Len() > s.rotateSize {
			cache.rotate()
		}
		cache.unlock()
	}

	return nil
}

func (s *rtStorage) Remove(c cid.Cid, provID peer.ID, pieceID cid.Cid) bool {
	in := store.IndexEntry{ProvID: provID, PieceID: pieceID}

	// Get multihash from cid
	k := string(c.Hash())

	cache := s.getCache(k)
	cache.lock()
	defer cache.unlock()

	return cache.remove(k, in)
}

// getCache returns the cache that stores the given key.  This function must
// evenly distribute keys over the set of caches.
func (s *rtStorage) getCache(k string) *syncCache {
	var idx int
	if k != "" {
		// Use last bits of key for good distribution
		//
		// bitwise modulus requires that size of cache set is power of 2
		idx = int(k[len(k)-1]) & (len(s.cacheSet) - 1)
	}
	return s.cacheSet[idx]
}

func (c *syncCache) lock()     { c.mutex.Lock() }
func (c *syncCache) unlock()   { c.mutex.Unlock() }
func (c *syncCache) size() int { return c.current.Len() + c.previous.Len() }

func (c *syncCache) get(k string) ([]store.IndexEntry, bool) {
	// Search current cache
	v, found := c.current.Get(k)
	if found {
		return v.([]store.IndexEntry), found
	}

	if c.previous == nil {
		return nil, false
	}

	// Search previous if not found in current
	v, found = c.previous.Get(k)

	// If nothing has been found return nil
	if !found {
		return nil, false
	}

	// Put the value found in the previous tree into the current one.
	c.current.Put(k, v)

	return v.([]store.IndexEntry), found
}

func (c *syncCache) put(k string, entry store.IndexEntry) bool {
	// Get from current or previous cache
	old, found := c.get(k)
	// If found it means there is already a value there.
	// Check if we are trying to put a duplicate entry
	// NOTE: If we end up having a lot of entries for the
	// same CID we may choose to change IndexEntry to a map[peer.ID]pieceID
	// to speed-up this lookup. Don't think is the case right now.
	if found && duplicateEntry(entry, old) {
		return false
	}

	c.current.Put(k, append(old, entry))
	return true
}

func (c *syncCache) remove(k string, entry store.IndexEntry) bool {
	removed := removeEntry(c.current, k, entry)
	if removeEntry(c.previous, k, entry) {
		removed = true
	}
	return removed
}

func removeEntry(cache *radixtree.Bytes, k string, entry store.IndexEntry) bool {
	// Get from current cache
	v, found := cache.Get(k)
	if !found {
		return false
	}

	values := v.([]store.IndexEntry)
	for i := range values {
		if entry.PieceID == values[i].PieceID &&
			entry.ProvID == values[i].ProvID {
			if len(values) == 1 {
				cache.Delete(k)
			} else {
				values[i] = values[len(values)-1]
				values[len(values)-1] = store.IndexEntry{}
				cache.Put(k, values[:len(values)-1])
			}
			return true
		}
	}
	return false
}

func (c *syncCache) removeAll(k string) bool {
	removed := c.current.Delete(k)
	if c.previous.Delete(k) {
		removed = true
	}
	return removed
}

func (c *syncCache) rotate() {
	c.previous, c.current = c.current, radixtree.New()
	c.rotations++
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
