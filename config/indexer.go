package config

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

// NewIndexer returns Indexer with values set to their defaults.
func NewIndexer() Indexer {
	return Indexer{
		CacheSize:      300000,
		ValueStoreDir:  "valuestore",
		ValueStoreType: "sth",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (cfg *Indexer) populateUnset() {
	defCfg := NewIndexer()

	if cfg.CacheSize == 0 {
		cfg.CacheSize = defCfg.CacheSize
	}
	if cfg.ValueStoreDir == "" {
		cfg.ValueStoreDir = defCfg.ValueStoreDir
	}
	if cfg.ValueStoreType == "" {
		cfg.ValueStoreType = defCfg.ValueStoreType
	}
}
