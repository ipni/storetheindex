package config

import "time"

// Ingest tracks the configuration related to the ingestion protocol.
type Ingest struct {
	// PubSubTopic used to advertise ingestion announcements.
	PubSubTopic string
	// StoreBatchSize is the number of entries in each write to the value
	// store.  Specifying a value less than 2 disables batching.
	StoreBatchSize int
	// SyncTimeout is the maximum amount of time allowed for a sync to complete
	// before it is canceled. This can be a sync of a chain of advertisements
	// or a chain of advertisement entries.  The value is an integer string
	// ending in "s", "m", "h" for seconds. minutes, hours.
	SyncTimeout Duration
}

// NewIngest returns Ingest with values set to their defaults.
func NewIngest() Ingest {
	return Ingest{
		PubSubTopic:    "/indexer/ingest/mainnet",
		StoreBatchSize: 256,
		SyncTimeout:    Duration(2 * time.Hour),
	}
}
