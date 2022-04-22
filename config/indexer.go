package config

import (
	"time"
)

// Indexer holds configuration for the indexer core.
type Indexer struct {
	// Maximum number of CIDs that cache can hold. Setting to 0 disables cache.
	CacheSize int
	// GCInterval configures the garbage collection interval for valuestores
	// that support it.
	GCInterval Duration
	// Directory where value store is kept. If this is not an absolute path
	// then the location is relative to the indexer repo directory.
	ValueStoreDir string
	// Type of valuestore to use, such as "sth" or "pogreb".
	ValueStoreType string
}

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:      300000,
		GCInterval:     Duration(30 * time.Minute),
		ValueStoreDir:  "valuestore",
		ValueStoreType: "sth",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Indexer) populateUnset() {
	def := NewIndexer()

	if c.CacheSize == 0 {
		c.CacheSize = def.CacheSize
	}
	if c.GCInterval == 0 {
		c.GCInterval = def.GCInterval
	}
	if c.ValueStoreDir == "" {
		c.ValueStoreDir = def.ValueStoreDir
	}
	if c.ValueStoreType == "" {
		c.ValueStoreType = def.ValueStoreType
	}
}
