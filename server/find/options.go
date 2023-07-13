package find

import (
	"fmt"
	"time"
)

const (
	defaultHomepage     = "https://web-ipni.cid.contact/"
	defaultMaxConns     = 8_000
	defaultReadTimeout  = 30 * time.Second
	defaultWriteTimeout = 30 * time.Second
)

// config contains all options for the server.
type config struct {
	homepageURL  string
	maxConns     int
	readTimeout  time.Duration
	writeTimeout time.Duration
	version      string
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		homepageURL:  defaultHomepage,
		maxConns:     defaultMaxConns,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithHomepage config for API.
func WithHomepage(URL string) Option {
	return func(c *config) error {
		c.homepageURL = URL
		return nil
	}
}

// MaxConnections config allowed by server.
func WithMaxConnections(maxConnections int) Option {
	return func(c *config) error {
		c.maxConns = maxConnections
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

// WithWriteTimeout configures server write timeout.
func WithWriteTimeout(t time.Duration) Option {
	return func(c *config) error {
		c.writeTimeout = t
		return nil
	}
}

// WithVersion sets the version string used in /health output.
func WithVersion(ver string) Option {
	return func(c *config) error {
		c.version = ver
		return nil
	}
}
