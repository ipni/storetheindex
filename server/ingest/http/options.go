package httpingestserver

import (
	"fmt"
	"time"
)

const (
	defaultWriteTimeout = 30 * time.Second
	defaultReadTimeout  = 30 * time.Second
)

// serverConfig contains all options for the server.
type serverConfig struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// Option is a function that sets a value in a serverConfig.
type Option func(*serverConfig) error

// getOpts creates a serverConfig and applies Options to it.
func getOpts(opts []Option) (serverConfig, error) {
	cfg := serverConfig{
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return serverConfig{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithReadTimeout serverConfigures server read timeout.
func WithReadTimeout(t time.Duration) Option {
	return func(c *serverConfig) error {
		c.readTimeout = t
		return nil
	}
}

// WithWriteTimeout serverConfigures server write timeout.
func WithWriteTimeout(t time.Duration) Option {
	return func(c *serverConfig) error {
		c.writeTimeout = t
		return nil
	}
}
