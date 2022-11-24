package handler

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
)

type (
	cachedStats struct {
		ctx    context.Context
		cancel context.CancelFunc

		indexer indexer.Interface
		ticker  *time.Ticker

		latest atomic.Value // Stores *latestStats
	}
	latestStats struct {
		entriesEstimate int64
		err             error
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
	if lastResult.err == nil {
		s, newResult.err = c.indexer.Stats()
	}
	switch newResult.err {
	case nil:
		newResult.entriesEstimate = int64(s.MultihashCount)
	case indexer.ErrStatsNotSupported:
		var size int64
		size, newResult.err = c.indexer.Size()
		if newResult.err == nil {
			newResult.entriesEstimate = size / avg_mh_size
		}
	}
	c.latest.Store(&newResult)
}

func (c *cachedStats) get() (s model.Stats, err error) {
	r := c.latest.Load().(*latestStats)
	err = r.err
	s.EntriesEstimate = r.entriesEstimate
	return
}

func (c *cachedStats) close() {
	c.ticker.Stop()
	c.cancel()
}
