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
	// FreezeAtPercent is the percent used, of the file system that
	// ValueStoreDir is on, at which to trigger the indexer to enter frozen
	// mode. A zero value uses the default. A negative value disables freezing.
	FreezeAtPercent float64
	// GCInterval configures the garbage collection interval for valuestores
	// that support it.
	GCInterval Duration
	// GCTimeLimit configures the maximum amount of time a garbage collection
	// cycle may run.
	GCTimeLimit Duration
	// IndexCountTotalAddend is a value that is added to the index count total,
	// to account for uncounted indexes that have existed in the value store
	// before provider index counts were tracked. This value is reloadable.
	IndexCountTotalAddend uint64
	// ShutdownTimeout is the duration that a graceful shutdown has to complete
	// before the daemon process is terminated. If unset or zero, configures no
	// shutdown timeout. This value is reloadable.
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
	// PebbleDisableWAL sets whether to disable write-ahead-log in Pebble which
	// can offer better performance in specific cases. Enabled by default. This
	// option only applies when ValueStoreType is set to "pebble".
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
		FreezeAtPercent:     95.0,
		GCInterval:          Duration(30 * time.Minute),
		GCTimeLimit:         Duration(5 * time.Minute),
		ShutdownTimeout:     0,
		ValueStoreDir:       "valuestore",
		ValueStoreType:      "pebble",
		STHBits:             24,
		STHBurstRate:        8 * 1024 * 1024,
		STHFileCacheSize:    512,
		STHSyncInterval:     Duration(time.Second),
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
	if c.FreezeAtPercent == 0 {
		c.FreezeAtPercent = def.FreezeAtPercent
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
}
