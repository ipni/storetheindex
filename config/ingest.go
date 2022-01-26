package config

import (
	"time"

	"github.com/ipld/go-ipld-prime/traversal/selector"
)

// Ingest tracks the configuration related to the ingestion protocol.
type Ingest struct {
	// PubSubTopic used to advertise ingestion announcements.
	PubSubTopic string
	// StoreBatchSize is the number of entries in each write to the value
	// store.  Specifying a value less than 2 disables batching.  This should
	// be smaller than the maximum number of multihashes in an entry block to
	// write concurrently to the value store.
	StoreBatchSize int
	// SyncTimeout is the maximum amount of time allowed for a sync to complete
	// before it is canceled. This can be a sync of a chain of advertisements
	// or a chain of advertisement entries.  The value is an integer string
	// ending in "s", "m", "h" for seconds. minutes, hours.
	SyncTimeout Duration

	// The recursion depth limit when syncing advertisement entries.
	// The value -1 means no limit. Defaults to 400.
	EntriesDepthLimit int64

	// The recursion depth limit when syncing advertisements.
	// The value -1 means no limit. Defaults to 400.
	AdvertisementDepthLimit int64
}

// NewIngest returns Ingest with values set to their defaults.
func NewIngest() Ingest {
	return Ingest{
		PubSubTopic:             "/indexer/ingest/mainnet",
		StoreBatchSize:          256,
		SyncTimeout:             Duration(2 * time.Hour),
		EntriesDepthLimit:       400,
		AdvertisementDepthLimit: 400,
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Ingest) populateUnset() {
	def := NewIngest()

	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
	if c.StoreBatchSize == 0 {
		c.StoreBatchSize = def.StoreBatchSize
	}
	if c.SyncTimeout == 0 {
		c.SyncTimeout = def.SyncTimeout
	}
	if c.EntriesDepthLimit == 0 {
		c.EntriesDepthLimit = def.EntriesDepthLimit
	}
	if c.AdvertisementDepthLimit == 0 {
		c.AdvertisementDepthLimit = def.AdvertisementDepthLimit
	}
}

// EntriesRecursionLimit returns the recursion limit of advertisement entries.
// See Ingest.EntriesDepthLimit
func (c *Ingest) EntriesRecursionLimit() selector.RecursionLimit {
	if c.EntriesDepthLimit == -1 {
		return selector.RecursionLimitNone()
	}
	return selector.RecursionLimitDepth(c.EntriesDepthLimit)
}

// AdvertisementRecursionLimit returns the recursion limit of advertisements.
// See Ingest.AdvertisementDepthLimit
func (c *Ingest) AdvertisementRecursionLimit() selector.RecursionLimit {
	if c.AdvertisementDepthLimit == -1 {
		return selector.RecursionLimitNone()
	}
	return selector.RecursionLimitDepth(c.AdvertisementDepthLimit)
}
