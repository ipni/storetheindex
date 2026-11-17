package loadgen

import (
	"context"
	"sync"

	"github.com/ipld/go-ipld-prime/storage/memstore"
)

// lockedStore is an in-memory block store that is safe for concurrent use.
// The load generator writes new advertisements to it while the publisher
// reads blocks from it to serve syncing indexers.
type lockedStore struct {
	mu    sync.RWMutex
	store memstore.Store
}

func newLockedStore() *lockedStore {
	// Bag is created here so that memstore reads never initialize it, which
	// lets Has and Get run under the read lock.
	return &lockedStore{store: memstore.Store{Bag: map[string][]byte{}}}
}

func (s *lockedStore) Has(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.Has(ctx, key)
}

func (s *lockedStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.Get(ctx, key)
}

func (s *lockedStore) Put(ctx context.Context, key string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Put(ctx, key, content)
}
