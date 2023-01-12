package registry

import (
	"errors"
	"fmt"

	"github.com/ipni/storetheindex/internal/registry/discovery"
)

// regConfig contains all options for the server.
type regConfig struct {
	discoverer      discovery.Discoverer
	freezeAtPercent float64
	valueStoreDir   string
}

// Option is a function that sets a value in a regConfig.
type Option func(*regConfig) error

// getOpts creates a regConfig and applies Options to it.
func getOpts(opts []Option) (regConfig, error) {
	var cfg regConfig
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return regConfig{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
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
