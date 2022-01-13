package config

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	// Type is the type of datastore.
	Type string
	// Dir is the directory where the datastore is kept. If this is not an
	// absolute path then the location is relative to the indexer repo
	// directory.
	Dir string
}

// NewDatastore returns Datastore with values set to their defaults.
func NewDatastore() Datastore {
	return Datastore{
		Type: "levelds",
		Dir:  "datastore",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Datastore) populateUnset() {
	def := NewDatastore()

	if c.Type == "" {
		c.Type = def.Type
	}
	if c.Dir == "" {
		c.Dir = def.Dir
	}
}
