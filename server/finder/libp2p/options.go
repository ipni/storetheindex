package p2pfinderserver

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
)

// NOTE: Placeholder to add options

// Protocol ID
const (
	pid0 protocol.ID = "/indexer/client/0.0.1"
)

// Options is a structure containing all the options that can be used when constructing p2pserver
type serverConfig struct {
}

// ServerOption type for p2pserver
type ServerOption func(*serverConfig) error

// defaults are the default options. This option will be automatically
// prepended to any options you pass to the constructor.
var serverDefaults = func(o *serverConfig) error {
	return nil
}

// apply applies the given options to this Option
func (c *serverConfig) apply(opts ...ServerOption) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("p2pserver option %d failed: %s", i, err)
		}
	}
	return nil
}
