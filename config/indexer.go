package config

const (
	defaultStoreType = "sth"
	defaultCacheSize = 300000
)

// Indexer holds configuration for the indexer core
type Indexer struct {
	StoreType string
	CacheSize int
}
