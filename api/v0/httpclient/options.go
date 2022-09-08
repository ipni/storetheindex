package httpclient

import (
	"fmt"
	"net/http"
	"time"
)

type clientConfig struct {
	timeout time.Duration
	client  *http.Client
}

// Option is the option type for httpclient
type Option func(*clientConfig) error

var clientDefaults = func(c *clientConfig) error {
	// As a fallback, never take more than a minute.
	// Most client API calls should use a context.
	c.timeout = time.Minute
	return nil
}

// apply applies the given options to this clientConfig
func (c *clientConfig) apply(opts ...Option) error {
	err := clientDefaults(c)
	if err != nil {
		// Failure of default option should panic
		panic("default option failed: " + err.Error())
	}
	for i, opt := range opts {
		if err = opt(c); err != nil {
			return fmt.Errorf("httpclient option %d failed: %s", i, err)
		}
	}
	return nil
}

// Timeout configures the timeout to wait for a response
func Timeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) error {
		cfg.timeout = timeout
		return nil
	}
}

// WithClient allows creation of the http client using an underlying network round tripper / client
func WithClient(c *http.Client) Option {
	return func(cfg *clientConfig) error {
		cfg.client = c
		return nil
	}
}
