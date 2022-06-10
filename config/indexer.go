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
	// ShutdownTimeout is the duration that a graceful shutdown has to complete
	// before the daemon process is terminated.
	ShutdownTimeout Duration
	// Directory where value store is kept. If this is not an absolute path
	// then the location is relative to the indexer repo directory.
	ValueStoreDir string
	// Type of valuestore to use, such as "sth" or "pogreb".
	ValueStoreType string
}

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:           300000,
		ConfigCheckInterval: Duration(30 * time.Minute),
		GCInterval:          Duration(30 * time.Minute),
		ShutdownTimeout:     Duration(10 * time.Second),
		ValueStoreDir:       "valuestore",
		ValueStoreType:      "sth",
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
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = def.ShutdownTimeout
	}
	if c.ValueStoreDir == "" {
		c.ValueStoreDir = def.ValueStoreDir
	}
	if c.ValueStoreType == "" {
		c.ValueStoreType = def.ValueStoreType
	}
}
