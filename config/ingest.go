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
	//
	// A chain of advertisements is synced by separate requests to the provider
	// for each advertisement. These requests are done in groups (segments) of
	// size set by SyncSegmentDepthLimit. AdvertisementDepthLimit sets the
	// limit on the total number of advertisements across all segments.
	AdvertisementDepthLimit int
	// CarMirrorDestination configures if, how, and where to store ingested
	// advertisements and entries in CAR files.
	CarMirrorDestination FileStore
	// EntriesDepthLimit is the total maximum recursion depth limit when
	// syncing advertisement entries. The value -1 means no limit and zero
	// means use the default value. The purpose is to prevent overload from
	// extremely long entry chains resulting from publisher misconfiguration.
	//
	// A chain of multihash entries chunks is synced by separate requests to
	// the provider for each chunk. These requests are done in groups
	// (segments) of size set by SyncSegmentDepthLimit. EntriesDepthLimit sets
	// the limit on the total number of entries chunks across all segments.
	EntriesDepthLimit int
	// HttpSyncRetryMax sets the maximum number of times HTTP sync requests
	// should be retried.
	HttpSyncRetryMax int
	// HttpSyncRetryWaitMax sets the maximum time to wait before retrying a
	// failed HTTP sync.
	HttpSyncRetryWaitMax Duration
	// HttpSyncRetryWaitMin sets the minimum time to wait before retrying a
	// failed HTTP sync.
	HttpSyncRetryWaitMin Duration
	// HttpSyncTimeout sets the time limit for HTTP sync requests.
	HttpSyncTimeout Duration
	// IngestWorkerCount sets how many ingest worker goroutines to spawn. This
	// controls how many concurrent ingest from different providers we can handle.
	IngestWorkerCount int
	// MinimumKeyLengt causes any multihash, that has a digest length less than
	// this, to be ignored. If using storethehash, this value is automatically
	// set to 4 if it was configured to be anything less.
	MinimumKeyLength int
	// PubSubTopic sets the topic name to which to subscribe for ingestion
	// announcements.
	PubSubTopic string
	// RateLimit contains rate-limiting configuration.
	RateLimit RateLimit
	// ResendDirectAnnounce determines whether or not to re-publish direct
	// announce messages over gossip pubsub. When a single indexer receives an
	// announce message via HTTP, enabling this lets the indexers re-publish
	// the announce so that other indexers can also receive it. This is always
	// false if configured to use an assigner.
	ResendDirectAnnounce bool
	// StoreBatchSize is the number of entries in each write to the value
	// store. Specifying a value less than 2 disables batching. This should be
	// smaller than the maximum number of multihashes in an entry block to
	// write concurrently to the value store.
	StoreBatchSize int
	// SyncSegmentDepthLimit is the depth limit of a single sync in a series of
	// calls that collectively sync advertisements or their entries. The value
	// -1 disables the segmentation where the sync will be done in a single call
	// and zero means use the default value.
	SyncSegmentDepthLimit int
	// SyncTimeout is the maximum amount of time allowed for a sync to complete
	// before it is canceled. This can be a sync of a chain of advertisements
	// or a chain of advertisement entries. The value is an integer string
	// ending in "s", "m", "h" for seconds. minutes, hours.
	SyncTimeout Duration
}

// FileStore configures a particular file store implementation.
type FileStore struct {
	// Compress specifies how to compress files. One of: "gzip", "none".
	// Defaults to "gzip" if unspecified.
	Compress string
	// Type of file store to use: "", "local", "s3"
	Type string
	// Configuration for storing files in local filesystem.
	Local LocalFileStore
	// Configuration for storing files in S3.
	S3 S3FileStore
}

type LocalFileStore struct {
	// Path to filesystem directory where files are stored.
	BasePath string
}

type S3FileStore struct {
	BucketName string

	// ## Optional Overrides ##
	//
	// These values are generally set by the environment and should only be
	// provided when necessary to override values from the environment, or when the
	// environment is not configured.
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
}

// NewIngest returns Ingest with values set to their defaults.
func NewIngest() Ingest {
	return Ingest{
		AdvertisementDepthLimit: 33554432,
		CarMirrorDestination: FileStore{
			Compress: "gzip",
		},
		EntriesDepthLimit:     65536,
		HttpSyncRetryMax:      4,
		HttpSyncRetryWaitMax:  Duration(30 * time.Second),
		HttpSyncRetryWaitMin:  Duration(1 * time.Second),
		HttpSyncTimeout:       Duration(10 * time.Second),
		IngestWorkerCount:     10,
		PubSubTopic:           "/indexer/ingest/mainnet",
		RateLimit:             NewRateLimit(),
		StoreBatchSize:        4096,
		SyncSegmentDepthLimit: 2_000,
		SyncTimeout:           Duration(2 * time.Hour),
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Ingest) populateUnset() {
	def := NewIngest()

	if c.AdvertisementDepthLimit == 0 {
		c.AdvertisementDepthLimit = def.AdvertisementDepthLimit
	}
	if c.CarMirrorDestination.Compress == "" {
		c.CarMirrorDestination.Compress = def.CarMirrorDestination.Compress
	}
	if c.EntriesDepthLimit == 0 {
		c.EntriesDepthLimit = def.EntriesDepthLimit
	}
	if c.HttpSyncRetryMax == 0 {
		c.HttpSyncRetryMax = def.HttpSyncRetryMax
	}
	if c.HttpSyncRetryWaitMax == 0 {
		c.HttpSyncRetryWaitMax = def.HttpSyncRetryWaitMax
	}
	if c.HttpSyncRetryWaitMin == 0 {
		c.HttpSyncRetryWaitMin = def.HttpSyncRetryWaitMin
	}
	if c.HttpSyncTimeout == 0 {
		c.HttpSyncTimeout = def.HttpSyncTimeout
	}
	if c.IngestWorkerCount == 0 {
		c.IngestWorkerCount = def.IngestWorkerCount
	}
	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
	c.RateLimit.populateUnset()
	if c.StoreBatchSize == 0 {
		c.StoreBatchSize = def.StoreBatchSize
	}
	if c.SyncSegmentDepthLimit == 0 {
		c.SyncSegmentDepthLimit = def.SyncSegmentDepthLimit
	}
	if c.SyncTimeout == 0 {
		c.SyncTimeout = def.SyncTimeout
	}
}
