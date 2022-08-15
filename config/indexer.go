package config

import (
	"time"
)

// Indexer holds configuration for the indexer core. Setting any of these items
// to their zero-value configures the default value.
type Indexer struct {
	// Maximum number of CIDs that cache can hold. Setting to -1 disables the
	// cache.
	CacheSize int
	// ConfigCheckInterval is the time between config file update checks.
	ConfigCheckInterval Duration
	// GCInterval configures the garbage collection interval for valuestores
	// that support it.
	GCInterval Duration
	// GCTimeLimit configures the maximum amount of time a garbage collection
	// cycle may run.
	GCTimeLimit Duration
	// ShutdownTimeout is the duration that a graceful shutdown has to complete
	// before the daemon process is terminated.
	ShutdownTimeout Duration
	// Directory where value store is kept. If this is not an absolute path
	// then the location is relative to the indexer repo directory.
	ValueStoreDir string
	// Type of valuestore to use, such as "sth" or "pogreb".
	ValueStoreType string
	// bits for bucket size in store the hash. Note: this should not be changed
	// from it's value at initialization or the datastore will be corrupted.
	STHBits uint8
}

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:           300000,
		ConfigCheckInterval: Duration(30 * time.Second),
		GCInterval:          Duration(30 * time.Minute),
		GCTimeLimit:         Duration(5 * time.Minute),
		ShutdownTimeout:     Duration(10 * time.Second),
		ValueStoreDir:       "valuestore",
		ValueStoreType:      "sth",
		STHBits:             24,
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
	if c.GCInterval == 0 {
		c.GCInterval = def.GCInterval
	}
	if c.GCTimeLimit == 0 {
		c.GCTimeLimit = def.GCTimeLimit
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = def.ShutdownTimeout
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
}
