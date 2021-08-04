package config

const (
	defaultAdminAddr  = "/ip4/127.0.0.1/tcp/3002"
	defaultFinderAddr = "/ip4/0.0.0.0/tcp/3000"
	defaultIngestAddr = "/ip4/0.0.0.0/tcp/3001"
)

// Addresses stores the (string) multiaddr addresses for the node.
type Addresses struct {
	// Admin is the admin API listen address
	Admin string
	// Finder is the finder API isten address
	Finder string
	// Ingest is the index data ingestion API listen address
	Ingest string
	// DisbleP2P disables libp2p hosting
	DisableP2P bool
}
