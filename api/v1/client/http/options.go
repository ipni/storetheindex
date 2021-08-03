package httpclient

import (
	"fmt"
)

// NOTE: Placeholder to add client options.

type clientConfig struct {
}

// ClientOption type for httpclient
type ClientOption func(*clientConfig) error

// apply applies the given options to this Option
func (c *clientConfig) apply(opts ...ClientOption) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("httpclient option %d failed: %s", i, err)
		}
	}
	return nil
}

var clientDefaults = func(o *clientConfig) error {
	return nil
}
