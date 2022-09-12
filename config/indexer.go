package config

import (
	"time"
)

// Indexer holds configuration for the indexer core. Setting any of these items
// to their zero-value configures the default value.
type Indexer struct {
	// CacheSize is the maximum number of CIDs that cache can hold. Setting to -1 disables the
	// cache.
	CacheSize int
	// ConfigCheckInterval is the time between config file update checks.
	ConfigCheckInterval Duration
	// CorePutConcurrency is the number of core goroutines used to write
	// individual multihashes within a Put. A value of 1 means no concurrency,
	// and zero uses the default.
	CorePutConcurrency int
	// GCInterval configures the garbage collection interval for valuestores
	// that support it.
	GCInterval Duration
	// GCTimeLimit configures the maximum amount of time a garbage collection
	// cycle may run.
	GCTimeLimit Duration
	// ShutdownTimeout is the duration that a graceful shutdown has to complete
	// before the daemon process is terminated.
	// A timeout of zero disables the shutdown timeout completely.
	// if unset, defaults to no shutdown timeout.
	ShutdownTimeout Duration
	// ValueStoreDir is the directory where value store is kept. If this is not
	// an absolute path then the location is relative to the indexer repo
	// directory.
	ValueStoreDir string
	// ValueStoreType specifies type of valuestore to use, such as "sth" or "pogreb".
	ValueStoreType string
	// STHBits is bits for bucket size in store the hash. Note: this should not be changed
	// from its value at initialization or the datastore will be corrupted.
	STHBits uint8
	// STHBurstRate specifies how much unwritten data can accumulate before
	// causing data to be flushed to disk.
	STHBurstRate uint64
	// STHFileCacheSize is the maximum number of open files that the STH file
	// cache may have. A value of 0 uses the default, and a value of -1
	// disables the file cache.
	STHFileCacheSize int
	// STHSyncInterval determines how frequently changes are flushed to disk.
	STHSyncInterval Duration
	// ValueStoreCodec configures the marshalling format of values stored by the valuestore.
	// It can be one of "json" or "binary". If unspecified, json format is used by default.
	//
	// Note that the format must not be changed after a valuestore is initialized. Changing
	// the format for a pre-existing valuestore will result in failure and potentially data
	// corruption.
	ValueStoreCodec string
	// PebbleDisableWAL sets whether to disable write-ahead-log in Pebble which can offer better
	// performance in specific cases. Enabled by default.
	// Note, this option is only considered when ValueStoreType type is set to "pebble".
	PebbleDisableWAL bool

	// TODO: If left unspecified, could the functionality instead be to use whatever the existing
	//      value store uses? If there is no existing value store, then use binary by default.
	//      For this we need probing mechanisms in go-indexer-core.
	//      While at it, do the same for STHBits.
}

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:           300000,
		ConfigCheckInterval: Duration(30 * time.Second),
		CorePutConcurrency:  64,
		GCInterval:          Duration(30 * time.Minute),
		GCTimeLimit:         Duration(5 * time.Minute),
		ShutdownTimeout:     0,
		ValueStoreDir:       "valuestore",
		ValueStoreType:      "sth",
		STHBits:             24,
		STHBurstRate:        4 * 1024 * 1024,
		STHFileCacheSize:    512,
		STHSyncInterval:     Duration(time.Second),
		ValueStoreCodec:     "json",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Indexer) populateUnset() {
	def := NewIndexer()

	if c.CacheSize == 0 {
		c.CacheSize = def.CacheSize
	}
	if c.ConfigCheckInterval == 0 {
		c.ConfigCheckInterval = def.ConfigCheckInterval
	}
	if c.CorePutConcurrency == 0 {
		c.CorePutConcurrency = def.CorePutConcurrency
	}
	if c.GCInterval == 0 {
		c.GCInterval = def.GCInterval
	}
	if c.GCTimeLimit == 0 {
		c.GCTimeLimit = def.GCTimeLimit
	}
	if c.ValueStoreDir == "" {
		c.ValueStoreDir = def.ValueStoreDir
	}
	if c.ValueStoreType == "" {
		c.ValueStoreType = def.ValueStoreType
	}
	if c.STHBits == 0 {
		c.STHBits = def.STHBits
	}
	if c.STHBurstRate == 0 {
		c.STHBurstRate = def.STHBurstRate
	}
	if c.STHFileCacheSize == 0 {
		c.STHFileCacheSize = def.STHFileCacheSize
	}
	if c.STHSyncInterval == 0 {
		c.STHSyncInterval = def.STHSyncInterval
	}
	if c.ValueStoreCodec == "" {
		c.ValueStoreCodec = def.ValueStoreCodec
	}
}
