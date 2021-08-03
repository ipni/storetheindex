package p2pclient

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
)

// NOTE: Placeholder to add client options

// Protocol ID
const (
	pid protocol.ID = "/indexer/client/0.0.1"
)

type clientConfig struct {
}

// ClientOption type for p2pclient
type ClientOption func(*clientConfig) error

// apply applies the given options to this Option
func (c *clientConfig) apply(opts ...ClientOption) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("p2pclient option %d failed: %s", i, err)
		}
	}
	return nil
}

var clientDefaults = func(o *clientConfig) error {
	return nil
}
