package config

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	// Dir is the directory where the datastore is kept. If this is not an
	// absolute path then the location is relative to the indexer repo
	// directory.
	Dir string
	// Type is the type of datastore.
	Type string
	// TempAdDir is the directory where the datastore for temporaty ad data is
	// kept. If this is not an absolute path then the location is relative to
	// the indexer repo.
	TempAdDir string
	// TempAdType is the type of datastore for temporary ad data.
	TempAdType string
}

// NewDatastore returns Datastore with values set to their defaults.
func NewDatastore() Datastore {
	return Datastore{
		Dir:        "datastore",
		Type:       "levelds",
		TempAdDir:  "tempadstore",
		TempAdType: "levelds",
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
	if c.TempAdDir == "" {
		c.TempAdDir = def.TempAdDir
	}
	if c.TempAdType == "" {
		c.TempAdType = def.TempAdType
	}
}
