package config

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

// NewAddresses returns Addresses with values set to their defaults.
func NewAddresses() Addresses {
	return Addresses{
		Admin:   "/ip4/127.0.0.1/tcp/3002",
		Finder:  "/ip4/0.0.0.0/tcp/3000",
		Ingest:  "/ip4/0.0.0.0/tcp/3001",
		P2PAddr: "/ip4/0.0.0.0/tcp/3003",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (cfg *Addresses) populateUnset() {
	defCfg := NewAddresses()

	if cfg.Admin == "" {
		cfg.Admin = defCfg.Admin
	}
	if cfg.Finder == "" {
		cfg.Finder = defCfg.Finder
	}
	if cfg.Ingest == "" {
		cfg.Ingest = defCfg.Ingest
	}
	if cfg.P2PAddr == "" {
		cfg.P2PAddr = defCfg.P2PAddr
	}
}
