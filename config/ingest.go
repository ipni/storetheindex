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
	// AdvertisementMirror configures if, how, and where to store content
	// advertisements data in CAR files. The mirror may be readable, writable,
	// both, or neither. If the mirror is neither readable or writable, or a
	// storage type is not specified, then the mirror is not used.
	AdvertisementMirror Mirror
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
	// FirstSyncDepth sets the advertisement chain depth to sync on the first
	// sync with a new provider. To sync a new provider with only the most
	// recent advertisement, set this to 1. A value of 0, the default, means
	// unlimited depth.
	FirstSyncDepth int
	// GsMaxInRequests is the maximum number of incoming in-progress graphsync
	// requests. Default is 1024.
	GsMaxInRequests uint64
	// GsMaxOutRequests is the maximum number of outgoing in-progress graphsync
	// requests. Default is 1024.
	GsMaxOutRequests uint64
	// HttpSyncRetryMax sets the maximum number of times HTTP sync requests
	// should be retried. A value of zero, the default, means no retry.
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
	// MaxAsyncConcurrency sets the maximum number of concurrent asynchrouous
	// syncs (started by announce messages). Set -1 for unlimited, 0 for
	// default. This value is reloadable.
	MaxAsyncConcurrency int
	// MinimumKeyLengt causes any multihash, that has a digest length less than
	// this, to be ignored.
	MinimumKeyLength int
	// OverwriteMirrorOnResync overwrites the advertisement when resyncing.
	OverwriteMirrorOnResync bool
	// PubSubTopic sets the topic name to which to subscribe for ingestion
	// announcements.
	PubSubTopic string
	// ResendDirectAnnounce determines whether or not to re-publish direct
	// announce messages over gossip pubsub. When a single indexer receives an
	// announce message via HTTP, enabling this lets the indexers re-publish
	// the announce so that other indexers can also receive it. This is always
	// false if configured to use an assigner.
	ResendDirectAnnounce bool
	// Skip500EntriesError, when true, skips advertisements for which the
	// publisher returns a 500 status code and an error message "failed to sync
	// first entry". This value is reloadable.
	Skip500EntriesError bool
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

// NewIngest returns Ingest with values set to their defaults.
func NewIngest() Ingest {
	return Ingest{
		AdvertisementDepthLimit: 33554432,
		AdvertisementMirror:     NewMirror(),
		EntriesDepthLimit:       65536,
		GsMaxInRequests:         1024,
		GsMaxOutRequests:        1024,
		HttpSyncRetryWaitMax:    Duration(30 * time.Second),
		HttpSyncRetryWaitMin:    Duration(1 * time.Second),
		HttpSyncTimeout:         Duration(10 * time.Second),
		IngestWorkerCount:       10,
		MaxAsyncConcurrency:     32,
		PubSubTopic:             "/indexer/ingest/mainnet",
		SyncSegmentDepthLimit:   2_000,
		SyncTimeout:             Duration(2 * time.Hour),
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Ingest) populateUnset() {
	def := NewIngest()

	c.AdvertisementMirror.PopulateUnset()

	if c.AdvertisementDepthLimit == 0 {
		c.AdvertisementDepthLimit = def.AdvertisementDepthLimit
	}
	if c.EntriesDepthLimit == 0 {
		c.EntriesDepthLimit = def.EntriesDepthLimit
	}
	if c.GsMaxInRequests == 0 {
		c.GsMaxInRequests = def.GsMaxInRequests
	}
	if c.GsMaxOutRequests == 0 {
		c.GsMaxOutRequests = def.GsMaxOutRequests
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
	if c.MaxAsyncConcurrency == 0 {
		c.MaxAsyncConcurrency = def.MaxAsyncConcurrency
	}
	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
	if c.SyncSegmentDepthLimit == 0 {
		c.SyncSegmentDepthLimit = def.SyncSegmentDepthLimit
	}
	if c.SyncTimeout == 0 {
		c.SyncTimeout = def.SyncTimeout
	}
}
