package carmirror

import (
	"fmt"
	"time"
)

const (
	defaultShutdownTimeout = 120 * time.Second
)

// config contains all options for the server.
type config struct {
	shutdownTimeout time.Duration
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		shutdownTimeout: defaultShutdownTimeout,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithShutdownTimeout configures server shutdown timeout.
func WithShutdownTimeout(t time.Duration) Option {
	return func(c *config) error {
		c.shutdownTimeout = t
		return nil
	}
}
