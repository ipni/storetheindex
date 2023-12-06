package config

import (
	"time"

	sticfg "github.com/ipni/storetheindex/config"
)

// DHStore holds configuration for the operating with dhstore nodes.
type DHStore struct {
	// DHBatchSize configures the batch size when sending batches of delete
	// requests to the DHStore service. A value < 1 results in the default
	// size.
	BatchSize int
	// DHStoreURL is the base URL for the DHStore service. This value is
	// required if ValueStoreType is "dhstore".
	URL string
	// DHStoreClusterURLs provide addional URLs that the core will send delete
	// requests to. Deletes will be send to the dhstoreURL as well as to all
	// dhstoreClusterURLs. This is required as deletes need to be applied to
	// all nodes until consistent hashing is implemented. dhstoreURL shouldn't
	// be included in this list.
	ClusterURLs []string
	// DHStoreHttpClientTimeout is a timeout for the DHStore http client.
	HttpClientTimeout sticfg.Duration
}

// NewDHStore returns DHstore with values set to their defaults.
func NewDHStore() DHStore {
	return DHStore{
		BatchSize:         16384,
		HttpClientTimeout: sticfg.Duration(30 * time.Second),
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *DHStore) populateUnset() {
	def := NewDHStore()
	if c.BatchSize < 1 {
		c.BatchSize = def.BatchSize
	}
	if c.HttpClientTimeout == 0 {
		c.HttpClientTimeout = def.HttpClientTimeout
	}
}
