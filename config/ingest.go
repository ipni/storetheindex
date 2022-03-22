package config

import (
	"time"
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

	// How many ingest worker goroutines to spawn. This controls how many
	// concurrent ingest from different providers we can handle.
	IngestWorkerCount int
}

// NewIngest returns Ingest with values set to their defaults.
func NewIngest() Ingest {
	return Ingest{
		PubSubTopic:       "/indexer/ingest/mainnet",
		StoreBatchSize:    4096,
		SyncTimeout:       Duration(2 * time.Hour),
		IngestWorkerCount: 10,
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
	if c.IngestWorkerCount == 0 {
		c.IngestWorkerCount = def.IngestWorkerCount
	}
}
