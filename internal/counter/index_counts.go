package counter

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
)

var log = logging.Logger("indexer/counters")

const (
	// indexCountPrefix identifies all provider index counts.
	indexCountPrefix = "/indexCounts/"
)

// IndexCounts persists multihash counts associated with provider ContextIDs.
// It is not safe to allow concurrent calls to IndexCounts methods for the same
// provider. Concurrent calls for different providers are safe.
type IndexCounts struct {
	ds datastore.Datastore
	// mutex protexts counts and total.
	mutex sync.Mutex
	// counts is in-mem total index counts for each provider.
	counts map[peer.ID]uint64
	// total is in-mem total index count for all providers.
	total uint64
	// totalAddend is a value that gets added to the total.
	totalAddend uint64
}

// NewIndexCounts creates a new IndexCounts given a Datastore.
func NewIndexCounts(ds datastore.Datastore) *IndexCounts {
	return &IndexCounts{
		ds:     ds,
		counts: make(map[peer.ID]uint64),
	}
}

// SetTotalAddent sets a value that is added to the index count returned by
// Total. Its purpose is to account for uncounted indexes that have existed
// since before provider index counts were tracked. This only affects Total,
// and does not affect any individual provider values.
func (c *IndexCounts) SetTotalAddend(totalAddend uint64) {
	c.mutex.Lock()
	c.totalAddend = totalAddend
	c.mutex.Unlock()
}

// AddCount adds the index count to the existing count for the for the
// provider's context iD, and updates in-memory totals.
func (c *IndexCounts) AddCount(providerID peer.ID, contextID []byte, count uint64) {
	if count == 0 {
		return
	}

	// Update in-mem values if they are present.
	c.mutex.Lock()
	prevCtxTotal, ok := c.counts[providerID]
	if ok {
		c.counts[providerID] = prevCtxTotal + count
	}
	if c.total != 0 {
		c.total += count
	}
	c.mutex.Unlock()

	key := makeIndexCountKey(providerID, contextID)

	// Get any previous count for this contextID and add to it.
	prevCtxCount, err := c.loadContextCount(context.Background(), key)
	if err != nil {
		return
	}
	err = c.ds.Put(context.Background(), key, varint.ToUvarint(prevCtxCount+count))
	if err != nil {
		log.Errorw("Cannot update index count", "err", err)
	}
}

// AddMissingCount stores the count only if there is no existing count for the
// provider's context ID, and updates in-memory totals.
func (c *IndexCounts) AddMissingCount(providerID peer.ID, contextID []byte, count uint64) {
	if count == 0 {
		return
	}

	key := makeIndexCountKey(providerID, contextID)

	has, err := c.ds.Has(context.Background(), key)
	if err != nil {
		log.Errorw("cannot load index count", "err", err)
		return
	}
	if has {
		return
	}
	err = c.ds.Put(context.Background(), key, varint.ToUvarint(count))
	if err != nil {
		log.Errorw("Cannot store index count", "err", err)
	}

	// Update in-mem values if they are present.
	c.mutex.Lock()
	prevCtxTotal, ok := c.counts[providerID]
	if ok {
		c.counts[providerID] = prevCtxTotal + count
	}
	if c.total != 0 {
		c.total += count
	}
	c.mutex.Unlock()
}

// RemoveCtx removes the index count for a provider's contextID.
func (c *IndexCounts) RemoveCtx(providerID peer.ID, contextID []byte) (uint64, error) {
	key := makeIndexCountKey(providerID, contextID)

	count, err := c.loadContextCount(context.Background(), key)
	if derr := c.ds.Delete(context.Background(), key); derr != nil {
		log.Errorw("Cannot delete index count", "err", derr)
	}
	if err != nil {
		return 0, err
	}

	// Update in-mem values if they are present.
	c.mutex.Lock()
	ptotal, ok := c.counts[providerID]
	if ok {
		if count < ptotal {
			c.counts[providerID] = ptotal - count
		} else {
			if count > ptotal {
				log.Error("Index count in data store is greater than in memory")
				count = ptotal
			}
			delete(c.counts, providerID)
		}
	}
	if c.total != 0 {
		c.total -= count
	}
	c.mutex.Unlock()

	return count, nil
}

// Provider reads all index counts for a provider.
func (c *IndexCounts) Provider(providerID peer.ID) (uint64, error) {
	// Return in-mem value if available.
	c.mutex.Lock()
	count, ok := c.counts[providerID]
	c.mutex.Unlock()
	if ok {
		return count, nil
	}

	total, err := c.loadProvider(context.Background(), providerID)
	if err != nil {
		return 0, err
	}

	// Track value in memory.
	c.mutex.Lock()
	c.counts[providerID] = total
	c.mutex.Unlock()

	return total, nil
}

// Total returns the total of all index counts for all providers.
func (c *IndexCounts) Total() (uint64, error) {
	// Return in-mem value if available.
	c.mutex.Lock()
	count := c.total
	total := count + c.totalAddend
	c.mutex.Unlock()
	if count != 0 {
		return total, nil
	}

	q := query.Query{
		Prefix: indexCountPrefix,
	}
	results, err := c.ds.Query(context.Background(), q)
	if err != nil {
		return 0, fmt.Errorf("cannot query index counts: %v", err)
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return 0, fmt.Errorf("cannot read index: %v", r.Error)
		}
		count, _, err = varint.FromUvarint(r.Entry.Value)
		if err != nil {
			log.Errorw("Cannot decode index count", "err", err)
			continue
		}

		total += count
	}

	// Track value in memory.
	c.mutex.Lock()
	c.total = total
	total += c.totalAddend
	c.mutex.Unlock()

	return total, nil
}

// RemoveProvider removes all index counts for a provider.
func (c *IndexCounts) RemoveProvider(providerID peer.ID) uint64 {
	var count uint64
	var ok bool

	c.mutex.Lock()
	if len(c.counts) != 0 {
		if c.total != 0 {
			count, ok = c.counts[providerID]
			if ok {
				c.total -= count
				delete(c.counts, providerID)
			}
		} else {
			ok = true
			delete(c.counts, providerID)
		}
	}
	c.mutex.Unlock()

	ctx := context.Background()
	if !ok {
		var err error
		count, err = c.loadProvider(ctx, providerID)
		if err != nil {
			c.mutex.Lock()
			c.total = 0
			c.mutex.Unlock()
			log.Errorw("Cannot load provider count", "err", err)
		}

		if count != 0 {
			c.mutex.Lock()
			if c.total != 0 {
				c.total -= count
			}
			c.mutex.Unlock()
		}
	}

	n, err := c.deletePrefix(ctx, indexCountPrefix+providerID.String())
	if err != nil {
		log.Errorw("Cannot delete provider contextID counts", "err", err)
	} else {
		log.Debugf("Removed %d contextID counts for provider", n)
	}

	return count
}

func (c *IndexCounts) loadContextCount(ctx context.Context, key datastore.Key) (uint64, error) {
	data, err := c.ds.Get(context.Background(), key)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("cannot load index count: %w", err)
	}
	count, _, err := varint.FromUvarint(data)
	if err != nil {
		return 0, fmt.Errorf("cannot decode index count: %w", err)
	}

	return count, nil
}

func (c *IndexCounts) loadProvider(ctx context.Context, providerID peer.ID) (uint64, error) {
	q := query.Query{
		Prefix: indexCountPrefix + providerID.String(),
	}
	results, err := c.ds.Query(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("cannot query all index counts: %v", err)
	}
	defer results.Close()

	var total uint64
	for r := range results.Next() {
		if r.Error != nil {
			return 0, fmt.Errorf("cannot read index: %v", r.Error)
		}
		count, _, err := varint.FromUvarint(r.Entry.Value)
		if err != nil {
			log.Errorw("Cannot decode index count", "err", err)
			continue
		}

		total += count
	}

	return total, nil
}

func makeIndexCountKey(provider peer.ID, contextID []byte) datastore.Key {
	var keyBuf strings.Builder
	keyBuf.WriteString(indexCountPrefix)
	keyBuf.WriteString(provider.String())
	keyBuf.WriteString("/")
	keyBuf.WriteString(base64.StdEncoding.EncodeToString(contextID))
	return datastore.NewKey(keyBuf.String())
}

func (c *IndexCounts) deletePrefix(ctx context.Context, prefix string) (int, error) {
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	}
	results, err := c.ds.Query(ctx, q)
	if err != nil {
		return 0, err
	}

	var delKeys []string
	for r := range results.Next() {
		delKeys = append(delKeys, r.Entry.Key)
	}
	results.Close()

	for _, key := range delKeys {
		err = c.ds.Delete(ctx, datastore.NewKey(key))
		if err != nil {
			return 0, err
		}
	}

	return len(delKeys), nil
}
