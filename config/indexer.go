package config

const (
	defaultCacheSize      = 300000
	defaultValueStoreType = "sth"
	defaultValueStoreDir  = "valuestore"
)

// Indexer holds configuration for the indexer core.
type Indexer struct {
	// Maximum number of CIDs that cache can hold. Setting to 0 disables cache.
	CacheSize int
	// Directory where value store is kept. If this is not an absolute path
	// then the location is relative to the indexer repo directory.
	ValueStoreDir string
	// Type of valuestore to use, such as "sti" or "pogreb".
	ValueStoreType string
}
