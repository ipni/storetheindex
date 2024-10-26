package registry

import (
	"errors"
	"fmt"
)

// regConfig contains all options for the server.
type regConfig struct {
	freezeAtPercent float64
	freezeDirs      []string
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

func WithFreezer(freezeDirs []string, freezeAtPercent float64) Option {
	return func(c *regConfig) error {
		if len(freezeDirs) != 0 && freezeAtPercent == 0 {
			return errors.New("cannot freeze at 0 percent usage")
		}
		c.freezeAtPercent = freezeAtPercent
		c.freezeDirs = freezeDirs
		return nil
	}
}
