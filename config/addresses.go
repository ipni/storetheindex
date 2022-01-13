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
func (c *Addresses) populateUnset() {
	def := NewAddresses()

	if c.Admin == "" {
		c.Admin = def.Admin
	}
	if c.Finder == "" {
		c.Finder = def.Finder
	}
	if c.Ingest == "" {
		c.Ingest = def.Ingest
	}
	if c.P2PAddr == "" {
		c.P2PAddr = def.P2PAddr
	}
}
