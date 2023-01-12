package httpclient

import (
	"fmt"
	"net/http"
	"time"
)

type ClientConfig struct {
	Timeout   time.Duration
	Client    *http.Client
	UseEncApi bool
}

// Option is the option type for httpclient
type Option func(*ClientConfig) error

var clientDefaults = func(c *ClientConfig) error {
	// As a fallback, never take more than a minute.
	// Most client API calls should use a context.
	c.Timeout = time.Minute
	// Use a normal API by default as it's supported by all flavours of IPNI
	c.UseEncApi = false
	return nil
}

// Apply applies the given options to this clientConfig
func (c *ClientConfig) Apply(opts ...Option) error {
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
	return func(cfg *ClientConfig) error {
		cfg.Timeout = timeout
		return nil
	}
}

// WithClient allows creation of the http client using an underlying network round tripper / client
func WithClient(c *http.Client) Option {
	return func(cfg *ClientConfig) error {
		cfg.Client = c
		return nil
	}
}

// WithUseEncApi allows switching client to private API (IPNI that it is talking to must support it)
func WithUseEncApi(b bool) Option {
	return func(cfg *ClientConfig) error {
		cfg.UseEncApi = b
		return nil
	}
}
