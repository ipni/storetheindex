package config

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	// AdTmpDir is the directory where the datastore for temporaty
	// advertisement data is kept. If this is not an absolute path then the
	// location is relative to the indexer repo.
	AdTmpDir string
	// AdTmpType is the type of datastore for temporary advertisement data.
	AdTmpType string
	// Dir is the directory where the datastore is kept. If this is not an
	// absolute path then the location is relative to the indexer repo
	// directory.
	Dir string
	// Type is the type of datastore.
	Type string
}

// NewDatastore returns Datastore with values set to their defaults.
func NewDatastore() Datastore {
	return Datastore{
		AdTmpDir:  "adtmpstore",
		AdTmpType: "levelds",
		Dir:       "datastore",
		Type:      "levelds",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Datastore) populateUnset() {
	def := NewDatastore()

	if c.Dir == "" {
		c.Dir = def.Dir
	}
	if c.Type == "" {
		c.Type = def.Type
	}
	if c.AdTmpDir == "" {
		c.AdTmpDir = def.AdTmpDir
	}
	if c.AdTmpType == "" {
		c.AdTmpType = def.AdTmpType
	}
}
