package registry

import (
	"errors"
	"fmt"

	"github.com/ipni/storetheindex/internal/registry/discovery"
)

// Options is a structure containing all the options that can be used when constructing an http server
type regConfig struct {
	discoverer      discovery.Discoverer
	freezeAtPercent float64
	valueStoreDir   string
}

// Option for registry
type Option func(*regConfig) error

// apply applies the given options to this Option.
func (c *regConfig) apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

func WithDiscoverer(discoverer discovery.Discoverer) Option {
	return func(c *regConfig) error {
		c.discoverer = discoverer
		return nil
	}
}

func WithFreezer(valueStoreDir string, freezeAtPercent float64) Option {
	return func(c *regConfig) error {
		if valueStoreDir != "" && freezeAtPercent == 0 {
			return errors.New("cannot freeze at 0 percent usage")
		}
		c.freezeAtPercent = freezeAtPercent
		c.valueStoreDir = valueStoreDir
		return nil
	}
}
