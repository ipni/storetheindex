package config

import (
	"time"
)

// Ingest tracks the configuration related to the ingestion protocol.
type Ingest struct {
	// AdvertisementDepthLimit is the total maximum recursion depth limit when
	// syncing advertisements. The value -1 means no limit and zero means use
	// the default value. Limiting the depth of advertisements can be done if
	// there is a need to prevent an indexer from ingesting long chains of
	// advertisements.
	// Note that sync is divided across multiple individual calls to a provider.
	// See SyncSegmentDepthLimit.
	AdvertisementDepthLimit int
	// EntriesDepthLimit is the total maximum recursion depth limit when syncing
	// advertisement entries. The value -1 means no limit and zero means use
	// the default value. The purpose is to prevent overload from extremely
	// long entry chains resulting from publisher misconfiguration.
	// Note that sync is divided across multiple individual calls to a provider.
	// See SyncSegmentDepthLimit.
	EntriesDepthLimit int
	// IngestWorkerCount sets how many ingest worker goroutines to spawn. This
	// controls how many concurrent ingest from different providers we can handle.
	IngestWorkerCount int
	// PubSubTopic sets the topic name to which to subscribe for ingestion
	// announcements.
	PubSubTopic string
	// RateLimit contains rate-limiting configuration.
	RateLimit RateLimit
	// StoreBatchSize is the number of entries in each write to the value
	// store. Specifying a value less than 2 disables batching. This should be
	// smaller than the maximum number of multihashes in an entry block to
	// write concurrently to the value store.
	StoreBatchSize int
	// SyncTimeout is the maximum amount of time allowed for a sync to complete
	// before it is canceled. This can be a sync of a chain of advertisements
	// or a chain of advertisement entries. The value is an integer string
	// ending in "s", "m", "h" for seconds. minutes, hours.
	SyncTimeout Duration
	// SyncSegmentDepthLimit is the depth limit of a single sync in a series of
	// calls that collectively sync advertisements or their entries. The value
	// -1 disables the segmentation where the sync will be done in a single call
	// and zero means use the default value.
	SyncSegmentDepthLimit int
}

// NewIngest returns Ingest with values set to their defaults.
func NewIngest() Ingest {
	return Ingest{
		AdvertisementDepthLimit: 33554432,
		EntriesDepthLimit:       65536,
		IngestWorkerCount:       10,
		PubSubTopic:             "/indexer/ingest/mainnet",
		RateLimit:               NewRateLimit(),
		StoreBatchSize:          4096,
		SyncTimeout:             Duration(2 * time.Hour),
		SyncSegmentDepthLimit:   2_000,
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Ingest) populateUnset() {
	def := NewIngest()

	if c.AdvertisementDepthLimit == 0 {
		c.AdvertisementDepthLimit = def.AdvertisementDepthLimit
	}
	if c.EntriesDepthLimit == 0 {
		c.EntriesDepthLimit = def.EntriesDepthLimit
	}
	if c.IngestWorkerCount == 0 {
		c.IngestWorkerCount = def.IngestWorkerCount
	}
	c.RateLimit.populateUnset()
	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
	if c.StoreBatchSize == 0 {
		c.StoreBatchSize = def.StoreBatchSize
	}
	if c.SyncTimeout == 0 {
		c.SyncTimeout = def.SyncTimeout
	}
	if c.SyncSegmentDepthLimit == 0 {
		c.SyncSegmentDepthLimit = def.SyncSegmentDepthLimit
	}
}
