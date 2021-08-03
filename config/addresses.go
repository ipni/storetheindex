package config

// Addresses stores the (string) multiaddr addresses for the node.
type Addresses struct {
	Admin     string // Address to listen on for admin API
	Discovery string // Address to listen on for provider discovery
	Ingest    string // Address to listen for index data ingestion
	Finder    string // Address to listen on for finder clients
}
