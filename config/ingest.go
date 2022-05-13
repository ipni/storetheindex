package config

import (
	"time"
)

// Ingest tracks the configuration related to the ingestion protocol.
type Ingest struct {
	// The recursion depth limit when syncing advertisements. The value -1
	// means no limit and zero means use the default value. Limiting the depth
	// of advertisements can be done if there is a need to prevent an indexer
	// from ingesting long chains of advertisements.
	AdvertisementDepthLimit int
	// The recursion depth limit when syncing advertisement entries. The value
	// -1 means no limit and zero means use the default value. The purpose is
	// to prevent overload from extremely long entry chains resulting from
	// publisher misconfiguration.
	EntriesDepthLimit int
	// How many ingest worker goroutines to spawn. This controls how many
	// concurrent ingest from different providers we can handle.
	IngestWorkerCount int
	// PubSubTopic used to advertise ingestion announcements.
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
	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
	if c.StoreBatchSize == 0 {
		c.StoreBatchSize = def.StoreBatchSize
	}
	if c.SyncTimeout == 0 {
		c.SyncTimeout = def.SyncTimeout
	}
}
