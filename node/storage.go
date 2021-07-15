package node

import (
	"fmt"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Storage = &nodeStorage{}

// nodeStorage combines a cache and a persistent storage
type nodeStorage struct {
	primary    store.Storage
	persistent store.PersistentStorage
}

// NewStorage creates new nodeStorage from a primary and persistent storages
func NewStorage(primary store.Storage, persistent store.PersistentStorage) *nodeStorage {
	return &nodeStorage{primary, persistent}
}

// Get retrieves IndexEntries for a CID
func (ns *nodeStorage) Get(c cid.Cid) ([]store.IndexEntry, bool, error) {
	if ns.primary != nil {
		// Check if CID in primary storage
		v, found, err := ns.primary.Get(c)
		if err != nil {
			return nil, false, err
		}

		if !found && ns.persistent != nil {
			v, found, err = ns.persistent.Get(c)
			if err != nil {
				return nil, false, err
			}
			// TODO: What about adding a CacheStorage interface that includes
			// putEntries(cid, []IndexEntry) function
			// so we don't need to loop through IndexEntry to move from
			// one storage to another?
			if found {
				// Move from persistent to cache
				for i := range v {
					_, err := ns.primary.Put(c, v[i])
					if err != nil {
						// Only log error since request has been satisified
						log.Errorw("failed to put entry into primary storage", "err", err)
						break
					}
				}
			}
		}
		return v, found, err
	}

	// If no primary storage, get from persistent
	return ns.persistent.Get(c)
}

// Put stores entry for a CID if the entry is not already
// stored.  New entries are added to the entries that are already there.
func (ns *nodeStorage) Put(c cid.Cid, entry store.IndexEntry) (bool, error) {
	return ns.put(c, entry)
}

func (ns *nodeStorage) put(c cid.Cid, entry store.IndexEntry) (bool, error) {
	// If there's only primary storage put right away
	if ns.persistent == nil {
		return ns.primary.Put(c, entry)
	}

	// If both check first if cid already in cache
	if ns.primary != nil {
		v, found, err := ns.primary.Get(c)
		if err != nil {
			return false, err
		}
		// If found, check if entry already exists in cache as if this
		// is the case there's no need to put anything new.
		if found {
			for i := range v {
				// If exists, no put needed here nor in persistent
				if v[i].Equal(entry) {
					return false, nil
				}
			}
			// Put in primary
			_, err := ns.primary.Put(c, entry)
			if err != nil {
				log.Errorw("failed to put entry into primary storage", "err", err)
			}
		}
	}

	// If persistent and not in cache we always put
	return ns.persistent.Put(c, entry)
}

// PutMany stores entry for multiple CIDs
func (ns *nodeStorage) PutMany(cs []cid.Cid, entry store.IndexEntry) error {
	// NOTE: We assume that if there is a persistent storage, new entries
	// are only added in persistence, and updated in cache if already there.
	// Under this assumption we can't use primary's PutManyCount despite being
	// more efficient than a single PUT because we would be putting
	// cids that were not in cache before.
	if ns.persistent != nil && ns.primary != nil {
		for i := range cs {
			_, err := ns.put(cs[i], entry)
			if err != nil {
				// TODO: Log error but don't return. Errors for a single
				// CID shouldn't stop from putting the rest.
				continue
			}
		}
		return nil
	}
	// If we have only persistence
	if ns.persistent != nil {
		return ns.persistent.PutMany(cs, entry)
	}
	// Only with cache
	return ns.primary.PutMany(cs, entry)
}

// Remove removes entry for a CID
func (ns *nodeStorage) Remove(c cid.Cid, entry store.IndexEntry) (bool, error) {
	var ok bool
	var err error
	// If there is persistent remove from persistent storage
	if ns.persistent != nil {
		ok, err = ns.persistent.Remove(c, entry)
		if err != nil {
			return false, err
		}
	} else {
		// If not we remove in primary storage right away
		return ns.primary.Remove(c, entry)
	}

	// If we have both, we need to check if there is an entry for
	// the cid in primary and remove it accordingly to keep both
	// storages consistent
	if ns.primary != nil && ok {
		return ns.primary.Remove(c, entry)
	}

	return ok, err
}

// RemoveMany removes entry from multiple CIDs
func (ns *nodeStorage) RemoveMany(cids []cid.Cid, entry store.IndexEntry) error {
	// Remove first from persistence
	if ns.persistent != nil {
		err := ns.persistent.RemoveMany(cids, entry)
		if err != nil {
			return err
		}
	}
	// If successfull the jump into primary
	if ns.primary != nil {
		err := ns.primary.RemoveMany(cids, entry)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveProvider removes all entries for specified provider.  This is used
// when a provider is no longer indexed by the indexer.
func (ns *nodeStorage) RemoveProvider(providerID peer.ID) error {
	// Remove first from persistence
	if ns.persistent != nil {
		err := ns.persistent.RemoveProvider(providerID)
		if err != nil {
			return err
		}
	}
	// If successfull the jump into primary
	if ns.primary != nil {
		err := ns.primary.RemoveProvider(providerID)
		if err != nil {
			return err
		}
	}

	return nil
}

// Size returns the total storage capacity being used
func (ns *nodeStorage) Size() (int64, error) {
	// If persistent exists, return total storage capacity
	if ns.persistent != nil {
		return ns.persistent.Size()
	}
	// NOTE: We won't return any Size in the combined storage for primary.
	// Size in primary currently returns the number of CIDs in memory, which
	// is not consistent with the value returned by persistent storages.
	return 0, fmt.Errorf("no persistence storage configured")
}
