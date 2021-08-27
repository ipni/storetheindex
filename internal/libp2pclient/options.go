package libp2pclient

import (
	"fmt"
)

// NOTE: Placeholder to add client options

type clientConfig struct {
}

// ClientOption type for p2pclient
type ClientOption func(*clientConfig) error

// apply applies the given options to this Option
func (c *clientConfig) apply(opts ...ClientOption) error {
	err := clientDefaults(c)
	if err != nil {
		// Failure of default option should panic
		panic("default option failed: " + err.Error())
	}
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
