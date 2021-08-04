package config

const (
	defaultCacheSize      = 300000
	defaultValueStoreType = "sth"
	defaultValueStoreDir  = "valuestore"
)

// Indexer holds configuration for the indexer core
type Indexer struct {
	// Maximum number of CIDs that cache can hold,0 to disable cache
	CacheSize int
	// Directory withing config root where value store is kept
	ValueStoreDir string
	// Type of value store to use
	ValueStoreType string
}
