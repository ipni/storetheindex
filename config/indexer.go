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
	// GCInterval configures the garbage collection interval for valuestores
	// that support it.
	GCInterval Duration
	// GCTimeLimit configures the maximum amount of time a garbage collection
	// cycle may run.
	GCTimeLimit Duration
	// GCScanFree rapidly scans for unused index files at the cost of bucket access.
	GCScanFree bool
	// ShutdownTimeout is the duration that a graceful shutdown has to complete
	// before the daemon process is terminated.
	// A timeout of zero disables the shutdown timeout completely.
	// if unset, defaults to no shutdown timeout.
	ShutdownTimeout Duration
	// ValueStoreDir is the directory where value store is kept. If this is not an absolute path
	// then the location is relative to the indexer repo directory.
	ValueStoreDir string
	// ValueStoreType specifies type of valuestore to use, such as "sth" or "pogreb".
	ValueStoreType string
	// STHBits is bits for bucket size in store the hash. Note: this should not be changed
	// from its value at initialization or the datastore will be corrupted.
	STHBits uint8
	// ValueStoreCodec configures the marshalling format of values stored by the valuestore.
	// It can be one of "json" or "binary". If unspecified, json format is used by default.
	//
	// Note that the format must not be changed after a valuestore is initialized. Changing
	// the format for a pre-existing valuestore will result in failure and potentially data
	// corruption.
	ValueStoreCodec string

	//TODO: If left unspecified, could the functionality instead be to use whatever the existing
	//      value store uses? If there is no existing value store, then use binary by default.
	//      For this we need probing mechanisms in go-indexer-core.
	//      While at it, do the same for STHBits.
}

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:           300000,
		ConfigCheckInterval: Duration(30 * time.Second),
		GCInterval:          Duration(30 * time.Minute),
		GCTimeLimit:         Duration(5 * time.Minute),
		GCScanFree:          false,
		ShutdownTimeout:     0,
		ValueStoreDir:       "valuestore",
		ValueStoreType:      "sth",
		STHBits:             24,
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
	if c.ValueStoreCodec == "" {
		c.ValueStoreCodec = def.ValueStoreCodec
	}
}
