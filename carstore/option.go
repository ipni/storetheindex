package carstore

import (
	"context"
	"fmt"
)

// config contains all options for the server.
type config struct {
	compAlg           string
	writeCheckContext context.Context
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	var cfg config
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	if cfg.writeCheckContext == nil {
		cfg.writeCheckContext = context.Background()
	}
	return cfg, nil
}

// WithCompress configures file compression.
func WithCompress(alg string) Option {
	return func(c *config) error {
		switch alg {
		case Gzip, "gz":
			c.compAlg = Gzip
		case "", "none", "nil", "null":
			c.compAlg = ""
		default:
			return fmt.Errorf("unsupported compression: %s", alg)
		}
		return nil
	}
}

// WithWriteCheckContext sets the context used by NewWriter when probing
// whether the file store is writable. The default is context.Background().
func WithWriteCheckContext(ctx context.Context) Option {
	return func(c *config) error {
		c.writeCheckContext = ctx
		return nil
	}
}
