package config

const (
	defaultCacheSize = 300000
	defaultStoreType = "sth"
)

// Indexer holds configuration for the indexer core
type Indexer struct {
	CacheSize int
	StoreType string
}
