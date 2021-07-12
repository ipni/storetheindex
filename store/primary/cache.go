package primary

import (
	"sync"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/gammazero/radixtree"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// syncCache is a rotatable cache. Multiples instances may be used to decrease
// concurrent access collision.
type syncCache struct {
	// CID -> IndexEntry
	current  *radixtree.Bytes
	previous *radixtree.Bytes

	// IndexEntery interning
	curEnts  *radixtree.Bytes
	prevEnts *radixtree.Bytes

	mutex     sync.Mutex
	rotations int
}

type CacheStats struct {
	Cids           int
	Values         int
	UniqueValues   int
	InternedValues int
	Rotations      int
}

// newSyncCache created a new syncCache instance
func newSyncCache() *syncCache {
	return &syncCache{
		current: radixtree.New(),
		curEnts: radixtree.New(),
	}
}

// stats returns the following:
//   - Number of CIDs stored in cache
//   - Number of IntexEentry values for all CIDs
//   - Number of unique IndexEntry values
//   - Number of interned IndexEntry values
//   - Number of cache rotations
//
// The number of unique and interred IndexEntry objects will be the same unless
// items have been removed from cache.
func (c *syncCache) stats() CacheStats {
	unique := make(map[*store.IndexEntry]struct{})
	var cidCount, valCount, interned int

	c.mutex.Lock()
	defer c.mutex.Unlock()

	walkFunc := func(k string, v interface{}) bool {
		values := v.([]*store.IndexEntry)
		valCount += len(values)
		for _, val := range values {
			unique[val] = struct{}{}
		}
		return false
	}

	cidCount = c.current.Len()
	c.current.Walk("", walkFunc)
	if c.previous != nil {
		cidCount += c.previous.Len()
		c.previous.Walk("", walkFunc)
	}

	interned = c.curEnts.Len()
	if c.prevEnts != nil {
		interned += c.prevEnts.Len()
	}

	return CacheStats{
		Cids:           cidCount,
		Values:         valCount,
		UniqueValues:   len(unique),
		InternedValues: interned,
		Rotations:      c.rotations,
	}
}

func (c *syncCache) get(k string) ([]*store.IndexEntry, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.getNoLock(k)
}

func (c *syncCache) getNoLock(k string) ([]*store.IndexEntry, bool) {
	// Search current cache
	v, found := c.current.Get(k)
	if !found {
		if c.previous == nil {
			return nil, false
		}

		// Search previous if not found in current
		v, found = c.previous.Get(k)
		if !found {
			return nil, false
		}

		// Pull the interned entries for these values forward from the previous
		// cache to keep using the same pointers as the va used in the cache
		// values that get pulled forward.
		values := v.([]*store.IndexEntry)
		for i, val := range values {
			values[i] = c.internEntry(val)
		}

		// Move the value found in the previous tree into the current one.
		c.current.Put(k, values)
		c.previous.Delete(k)
	}
	return v.([]*store.IndexEntry), true
}

// put stores an entry in the cache if the entry is not already stored.
// Returns true if a new entry was added to the cache.
func (c *syncCache) put(k string, entry store.IndexEntry, rotateSize int) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Get from current or previous cache
	old, found := c.getNoLock(k)
	// If found values(s) then check the value to put is already there.
	if found && duplicateEntry(entry, old) {
		return false
	}

	if c.current.Len() >= rotateSize {
		c.rotate()
	}

	c.current.Put(k, append(old, c.internEntry(&entry)))
	return true
}

func (c *syncCache) putInterned(k string, ent *store.IndexEntry, rotateSize int) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	old, found := c.getNoLock(k)
	if found && duplicateEntry(*ent, old) {
		return false
	}

	if c.current.Len() >= rotateSize {
		c.rotate()
	}

	c.current.Put(k, append(old, ent))
	return true
}

func (c *syncCache) putMany(keys []string, entry store.IndexEntry, rotateSize int) int {
	var count int
	var ent *store.IndexEntry

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, k := range keys {
		old, found := c.getNoLock(k)
		if found && duplicateEntry(entry, old) {
			continue
		}

		if c.current.Len() > rotateSize {
			c.rotate()
		}

		if ent == nil {
			ent = c.internEntry(&entry)
		}
		c.current.Put(k, append(old, ent))
		count++
	}

	return count
}

func (c *syncCache) remove(k string, entry store.IndexEntry) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	removed := removeEntry(c.current, k, entry)
	if c.previous != nil && removeEntry(c.previous, k, entry) {
		removed = true
	}
	return removed
}

func (c *syncCache) removeProvider(providerID peer.ID) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	count := removeProviderEntries(c.current, providerID)
	removeProviderInterns(c.curEnts, providerID)
	if c.previous != nil {
		count += removeProviderEntries(c.previous, providerID)
		if c.prevEnts != nil {
			removeProviderInterns(c.prevEnts, providerID)
		}
	}
	return count
}

func (c *syncCache) rotate() {
	c.previous, c.current = c.current, radixtree.New()
	c.prevEnts, c.curEnts = c.curEnts, radixtree.New()
	c.rotations++
}

func (c *syncCache) internEntry(entry *store.IndexEntry) *store.IndexEntry {
	k := string(entry.ProvID) + cidToKey(entry.PieceID)
	v, found := c.curEnts.Get(k)
	if !found {
		if c.prevEnts != nil {
			v, found = c.prevEnts.Get(k)
			if found {
				// Pull interned entry forward from previous cache
				c.curEnts.Put(k, v)
				c.prevEnts.Delete(k)
				return v.(*store.IndexEntry)
			}
		}
		// Intern new entry
		c.curEnts.Put(k, entry)
		return entry
	}
	// Found existing interned entry
	return v.(*store.IndexEntry)
}

func removeEntry(tree *radixtree.Bytes, k string, entry store.IndexEntry) bool {
	// Get from current cache
	v, found := tree.Get(k)
	if !found {
		return false
	}

	values := v.([]*store.IndexEntry)
	for i, v := range values {
		if v.ProvID == entry.ProvID && v.PieceID == entry.PieceID {
			if len(values) == 1 {
				tree.Delete(k)
			} else {
				values[i] = values[len(values)-1]
				values[len(values)-1] = nil
				tree.Put(k, values[:len(values)-1])
			}
			return true
		}
	}
	return false
}

func removeProviderEntries(tree *radixtree.Bytes, providerID peer.ID) int {
	var count int
	var deletes []string

	tree.Walk("", func(k string, v interface{}) bool {
		values := v.([]*store.IndexEntry)
		for i := range values {
			if providerID == values[i].ProvID {
				count++
				if len(values) == 1 {
					deletes = append(deletes, k)
				} else {
					values[i] = values[len(values)-1]
					values[len(values)-1] = nil
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

func removeProviderInterns(tree *radixtree.Bytes, providerID peer.ID) {
	var deletes []string
	tree.Walk(string(providerID), func(k string, v interface{}) bool {
		deletes = append(deletes, k)
		return false
	})
	for _, k := range deletes {
		tree.Delete(k)
	}
}

// Checks if the entry already exists in the index. An entry
// for the same provider but a different piece is not considered
// a duplicate entry (at least for now)
func duplicateEntry(newEnt store.IndexEntry, oldEnts []*store.IndexEntry) bool {
	for _, oldEnt := range oldEnts {
		if newEnt.PieceID == oldEnt.PieceID && newEnt.ProvID == oldEnt.ProvID {
			return true
		}
	}
	return false
}
