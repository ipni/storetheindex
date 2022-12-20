package server

import (
	"fmt"
	"time"
)

const (
	defaultWriteTimeout = 30 * time.Second
	defaultReadTimeout  = 30 * time.Second
)

// config contains all options for the server.
type config struct {
	writeTimeout time.Duration
	readTimeout  time.Duration
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts ...Option) (config, error) {
	cfg := config{
		writeTimeout: defaultWriteTimeout,
		readTimeout:  defaultReadTimeout,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithWriteTimeout configures server write timeout.
func WithWriteTimeout(t time.Duration) Option {
	return func(c *config) error {
		c.writeTimeout = t
		return nil
	}
}

// WithReadTimeout configures server read timeout.
func WithReadTimeout(t time.Duration) Option {
	return func(c *config) error {
		c.readTimeout = t
		return nil
	}
}
