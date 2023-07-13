package config

import (
	"time"
)

// Indexer holds configuration for the indexer core. Setting any of these items
// to their zero-value configures the default value.
type Indexer struct {
	// CacheSize is the maximum number of CIDs that cache can hold. Setting to
	// -1 disables the cache.
	CacheSize int
	// ConfigCheckInterval is the time between config file update checks.
	ConfigCheckInterval Duration
	// DHBatchSize configures the batch size when sending batches of merge
	// requests to the DHStore service. A value < 1 results in the default
	// size.
	DHBatchSize int
	// DHStoreURL is the base URL for the DHStore service. This value is required
	// if ValueStoreType is "dhstore".
	DHStoreURL string
	// DHStoreClusterURLs provide addional URLs that the core will send delete
	// requests to. Deletes will be send to the dhstoreURL as well as to all
	// dhstoreClusterURLs. This is required as deletes need to be applied to
	// all nodes until consistent hashing is implemented. dhstoreURL shouldn't
	// be included in this list.
	DHStoreClusterURLs []string
	// DHStoreHttpClientTimeout is a timeout for the DHStore http client
	DHStoreHttpClientTimeout Duration
	// FreezeAtPercent is the percent used, of the file system that
	// ValueStoreDir is on, at which to trigger the indexer to enter frozen
	// mode. A zero value uses the default. A negative value disables freezing.
	FreezeAtPercent float64
	// ShutdownTimeout is the duration that a graceful shutdown has to complete
	// before the daemon process is terminated. If unset or zero, configures no
	// shutdown timeout. This value is reloadable.
	ShutdownTimeout Duration
	// ValueStoreDir is the directory where value store is kept, if the value
	// store type requires local storage. If this is not an absolute path then
	// the location is relative to the indexer repo directory.
	ValueStoreDir string
	// ValueStoreType specifies type of valuestore to use, such as "dhstore" or
	// "pebble". If no set, then defaults to "dhstore" if DHStoreURL is
	// configures, otherwise defaults to "pebble".
	ValueStoreType string
	// PebbleDisableWAL sets whether to disable write-ahead-log in Pebble which
	// can offer better performance in specific cases. Enabled by default. This
	// option only applies when ValueStoreType is set to "pebble".
	PebbleDisableWAL bool
	// PebbleBlockCacheSize is a size of pebble block cache in bytes
	PebbleBlockCacheSize ByteSize
	// UnfreezeOnStart tells that indexer to unfreeze itself on startup if it
	// is frozen. This reverts the indexer to the state it was in before it was
	// frozen. It only retains the most recent provider and publisher
	// addresses.
	UnfreezeOnStart bool
}

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:            300000,
		PebbleBlockCacheSize: 1 << 30, // 1 Gi
		ConfigCheckInterval:  Duration(30 * time.Second),
		FreezeAtPercent:      90.0,
		ShutdownTimeout:      0,
		ValueStoreDir:        "valuestore",
		ValueStoreType:       "pebble",
		// defaulting http timeout to 10 seconds to survive over occasional
		// spikes caused by compaction
		DHStoreHttpClientTimeout: Duration(10 * time.Second),
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
	if c.FreezeAtPercent == 0 {
		c.FreezeAtPercent = def.FreezeAtPercent
	}
	if c.ValueStoreDir == "" {
		c.ValueStoreDir = def.ValueStoreDir
	}
	if c.ValueStoreType == "" || c.ValueStoreType == "none" {
		if c.DHStoreURL != "" {
			c.ValueStoreType = "dhstore"
		} else {
			c.ValueStoreType = def.ValueStoreType
		}
	}
	if c.DHStoreHttpClientTimeout == 0 {
		c.DHStoreHttpClientTimeout = def.DHStoreHttpClientTimeout
	}
	if c.PebbleBlockCacheSize == 0 {
		c.PebbleBlockCacheSize = def.PebbleBlockCacheSize
	}
}
