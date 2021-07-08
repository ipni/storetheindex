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

// rtStorage is Adaptive Radix Tree based primary storage
type rtStorage struct {
	cacheSet   []*syncCache
	rotateSize int

	//providerPieces *radixtree.Bytes
}

// cidToKey gets the multihash from a CID to be used as a cache key
func cidToKey(c cid.Cid) string { return string(c.Hash()) }

// New creates a new Adaptive Radix Tree storage
func New(size int) *rtStorage {
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

func (s *rtStorage) Get(c cid.Cid) ([]store.IndexEntry, bool, error) {
	// Keys indexed as multihash
	k := cidToKey(c)

	cache := s.getCache(k)
	cache.lock()
	defer cache.unlock()

	ents, found := cache.get(k)
	return ents, found, nil
}

func (s *rtStorage) Put(c cid.Cid, providerID peer.ID, pieceID cid.Cid) error {
	s.PutCheck(c, providerID, pieceID)
	return nil
}

// PutCheck stores a provider-piece entry for a CID if the entry is not already
// stored.  New entries are added to the entries that are already there.
// Returns true if a new entry was added to the cache.
func (s *rtStorage) PutCheck(c cid.Cid, providerID peer.ID, pieceID cid.Cid) bool {
	in := store.IndexEntry{ProvID: providerID, PieceID: pieceID}
	k := cidToKey(c)

	cache := s.getCache(k)
	cache.lock()
	stored := cache.put(k, in)
	if stored && cache.current.Len() > s.rotateSize {
		// Only rotate one cache at a time. This may leave older entries in
		// other caches, but if CIDs are dirstributed evenly over the cache set
		// then over time all members should be rotated the same amount on
		// average.  This is done so that it is not necessary to lock all
		// caches in order to perform a rotation.  This also means that items
		// age out more incrementally.
		cache.rotate()
	}
	cache.unlock()

	return stored
}

func (s *rtStorage) PutMany(cids []cid.Cid, providerID peer.ID, pieceID cid.Cid) error {
	s.PutManyCount(cids, providerID, pieceID)
	return nil
}

// PutManyCount stores the provider-piece entry for multiple CIDs.  Returns the
// number of new entries stored.
func (s *rtStorage) PutManyCount(cids []cid.Cid, providerID peer.ID, pieceID cid.Cid) int {
	var stored int
	in := store.IndexEntry{ProvID: providerID, PieceID: pieceID}

	for i := range cids {
		k := cidToKey(cids[i])
		cache := s.getCache(k)
		cache.lock()
		if cache.put(k, in) {
			stored++
			if cache.current.Len() > s.rotateSize {
				cache.rotate()
			}
		}
		cache.unlock()
	}

	return stored
}

func (s *rtStorage) Remove(c cid.Cid, providerID peer.ID, pieceID cid.Cid) error {
	s.RemoveCheck(c, providerID, pieceID)
	return nil
}

// RemoveCheck removes a provider-piece entry for a CID.  Returns true if an
// entry was removed from cache.
func (s *rtStorage) RemoveCheck(c cid.Cid, providerID peer.ID, pieceID cid.Cid) bool {
	in := store.IndexEntry{ProvID: providerID, PieceID: pieceID}
	k := cidToKey(c)

	cache := s.getCache(k)
	cache.lock()
	defer cache.unlock()

	return cache.remove(k, in)
}

func (s *rtStorage) RemoveMany(cids []cid.Cid, providerID peer.ID, pieceID cid.Cid) error {
	s.RemoveManyCount(cids, providerID, pieceID)
	return nil
}

// RemoveManyCount removes a provider-piece entry from multiple CIDs.  Returns
// the number of entries removed.
func (s *rtStorage) RemoveManyCount(cids []cid.Cid, providerID peer.ID, pieceID cid.Cid) int {
	var removed int
	in := store.IndexEntry{ProvID: providerID, PieceID: pieceID}

	for i := range cids {
		k := cidToKey(cids[i])
		cache := s.getCache(k)
		cache.lock()
		if cache.remove(k, in) {
			removed++
		}
		cache.unlock()
	}

	return removed
}

func (s *rtStorage) RemoveProvider(providerID peer.ID) error {
	s.RemoveProvider(providerID)
	return nil
}

// RemoveProvider removes all enrties for specified provider.  Returns the
// total number of entries reomved from the cache.
func (s *rtStorage) RemoveProviderCount(providerID peer.ID) int {
	var count int
	for _, cache := range s.cacheSet {
		cache.lock()
		count += cache.removeProvider(providerID)
		cache.unlock()
	}
	return count
}

func (s *rtStorage) CidCount() int {
	var count int
	for _, cache := range s.cacheSet {
		cache.lock()
		count += cache.cidCount()
		cache.unlock()
	}
	return count
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

func (c *syncCache) lock()   { c.mutex.Lock() }
func (c *syncCache) unlock() { c.mutex.Unlock() }
func (c *syncCache) cidCount() int {
	size := c.current.Len()
	if c.previous != nil {
		size += c.previous.Len()
	}
	return size
}

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

	// Move the value found in the previous tree into the current one.
	c.current.Put(k, v)
	c.previous.Delete(k)

	return v.([]store.IndexEntry), found
}

// put stores an entry in the cache if the entry is not already stored.
// Returns true if a new entry was added to the cache.
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
	if c.previous != nil && removeEntry(c.previous, k, entry) {
		removed = true
	}
	return removed
}

func (c *syncCache) removeProvider(providerID peer.ID) int {
	count := removeProviderEntry(c.current, providerID)
	if c.previous != nil {
		count += removeProviderEntry(c.previous, providerID)
	}
	return count
}

func (c *syncCache) rotate() {
	c.previous, c.current = c.current, radixtree.New()
	c.rotations++
}

func removeEntry(tree *radixtree.Bytes, k string, entry store.IndexEntry) bool {
	// Get from current cache
	v, found := tree.Get(k)
	if !found {
		return false
	}

	values := v.([]store.IndexEntry)
	for i := range values {
		if entry.PieceID == values[i].PieceID &&
			entry.ProvID == values[i].ProvID {
			if len(values) == 1 {
				tree.Delete(k)
			} else {
				values[i] = values[len(values)-1]
				values[len(values)-1] = store.IndexEntry{}
				tree.Put(k, values[:len(values)-1])
			}
			return true
		}
	}
	return false
}

func removeProviderEntry(tree *radixtree.Bytes, providerID peer.ID) int {
	var count int
	var deletes []string

	tree.Walk("", func(k string, v interface{}) bool {
		values := v.([]store.IndexEntry)
		for i := range values {
			if providerID == values[i].ProvID {
				count++
				if len(values) == 1 {
					deletes = append(deletes, k)
				} else {
					values[i] = values[len(values)-1]
					values[len(values)-1] = store.IndexEntry{}
					tree.Put(k, values[:len(values)-1])
				}
			}
		}
		return false
	})

	for _, k := range deletes {
		tree.Delete(k)
	}

	return count
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
