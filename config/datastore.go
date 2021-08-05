package config

const (
	defaultDatastoreType = "levelds"
	defaultDatastoreDir  = "datastore"
)

// Datastore tracks the configuration of the datastore.
type Datastore struct {
	// Type is the type of datastore
	Type string
	// Dir is the directory withing the config root where the datastore is kept
	Dir string
}
