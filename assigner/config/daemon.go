package config

// Daemon stores daemon settings.
type Daemon struct {
	// HTTPAddr is the HTTP host multiaddr for receiving direct announce
	// messages. Set to "none" to disable HTTP hosting.
	HTTPAddr string
	// P2PAddr is the libp2p host multiaddr for receiving announce messages. Set to "none" to
	// disable libp2p hosting.
	P2PAddr string
	// NoResourceManager disables the libp2p resource manager when true.
	NoResourceManager bool
}

// NewDaemon returns Addresses with values set to their defaults.
func NewDaemon() Daemon {
	return Daemon{
		HTTPAddr: "/ip4/0.0.0.0/tcp/3001",
		P2PAddr:  "/ip4/0.0.0.0/tcp/3000",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Daemon) populateUnset() {
	def := NewDaemon()

	if c.HTTPAddr == "" {
		c.HTTPAddr = def.HTTPAddr
	}
	if c.P2PAddr == "" {
		c.P2PAddr = def.P2PAddr
	}
}
