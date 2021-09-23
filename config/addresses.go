package config

const (
	defaultAdminAddr  = "/ip4/127.0.0.1/tcp/3002"
	defaultFinderAddr = "/ip4/0.0.0.0/tcp/3000"
	defaultIngestAddr = "/ip4/0.0.0.0/tcp/3001"
	defaultP2PAddr    = "/ip4/0.0.0.0/tcp/3003"
)

// Addresses stores the (string) multiaddr addresses for the node.
type Addresses struct {
	// Admin is the admin http listen address
	Admin string
	// Finder is the finder http isten address
	Finder string
	// Ingest is the index data ingestion http listen address
	Ingest string
	// DisbleP2P disables libp2p hosting
	DisableP2P bool
	// P2PMaddr is the libp2p host multiaddr for all servers
	P2PAddr string
}
