package config

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	// Dir is the directory where the datastore is kept. If this is not an
	// absolute path then the location is relative to the indexer repo
	// directory.
	Dir string
	// TmpDir is the directory where the datastore for persisted temporaty data
	// is kept. This datastore contains temporary items such as synced
	// advertisement data and data-transfer session state. If this is not an
	// absolute path then the location is relative to the indexer repo.
	TmpDir string
}

// NewDatastore returns Datastore with values set to their defaults.
func NewDatastore() Datastore {
	return Datastore{
		Dir:    "gcdatastore",
		TmpDir: "gctmpstore",
	}
}

// PopulateUnset replaces zero-values in the config with default values.
func (c *Datastore) PopulateUnset() {
	def := NewDatastore()

	if c.Dir == "" {
		c.Dir = def.Dir
	}
	if c.TmpDir == "" {
		c.TmpDir = def.TmpDir
	}
}
