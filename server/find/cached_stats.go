package find

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/find/model"
)

// avgMhSize is a slight overcount over the expected size of a multihash as a
// way of estimating the number of entries in the primary value store.
const avgMhSize = 40

type (
	cachedStats struct {
		ctx    context.Context
		cancel context.CancelFunc

		indexer indexer.Interface
		ticker  *time.Ticker

		latest atomic.Value // Stores *latestStats
	}
	latestStats struct {
		entriesEstimate    int64
		entriesCount       int64
		errEntriesEstimate error
		errEntriesCount    error
	}
)

func newCachedStats(indexer indexer.Interface, tick time.Duration) *cachedStats {
	c := &cachedStats{
		indexer: indexer,
		ticker:  time.NewTicker(tick),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.latest.Store(&latestStats{})
	c.start()
	return c
}

func (c *cachedStats) start() {
	go func() {
		c.refresh()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-c.ticker.C:
				c.refresh()
			}
		}
	}()
}

func (c *cachedStats) refresh() {
	lastResult := c.latest.Load().(*latestStats)
	var s *indexer.Stats
	var newResult latestStats
	// Only check the `stats` endpoint once; if the valuestore does not support it there is no
	// point checking it at every cycle.
	if lastResult.errEntriesCount != indexer.ErrStatsNotSupported {
		s, newResult.errEntriesCount = c.indexer.Stats()
		if newResult.errEntriesCount == nil && s != nil {
			newResult.entriesCount = int64(s.MultihashCount)
		}
	}
	var size int64
	size, newResult.errEntriesEstimate = c.indexer.Size()
	if newResult.errEntriesEstimate == nil {
		newResult.entriesEstimate = size / avgMhSize
	}
	c.latest.Store(&newResult)
}

func (c *cachedStats) get() (s model.Stats, err error) {
	r := c.latest.Load().(*latestStats)
	s.EntriesEstimate = r.entriesEstimate
	s.EntriesCount = r.entriesCount

	if r.errEntriesCount != nil && r.errEntriesCount != indexer.ErrStatsNotSupported {
		log.Warn("Failed to get EntriesCount", "err", r.errEntriesCount)
	}

	err = r.errEntriesEstimate
	return
}

func (c *cachedStats) close() {
	c.ticker.Stop()
	c.cancel()
}
