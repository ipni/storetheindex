package adminserver

import (
	"fmt"
	"time"
)

const (
	apiWriteTimeout = 30 * time.Second
	apiReadTimeout  = 30 * time.Second
)

// Options is a structure containing all the options that can be used when constructing an http server
type serverConfig struct {
	apiWriteTimeout time.Duration
	apiReadTimeout  time.Duration
}

// ServerOption for httpserver
type ServerOption func(*serverConfig) error

// defaults are the default ptions. This option will be automatically
// prepended to any options you pass to the constructor.
var serverDefaults = func(o *serverConfig) error {
	o.apiWriteTimeout = apiWriteTimeout
	o.apiReadTimeout = apiReadTimeout
	return nil
}

// apply applies the given options to this Option
func (c *serverConfig) apply(opts ...ServerOption) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("httpserver option %d failed: %s", i, err)
		}
	}
	return nil
}

// WriteTimeout config for API
func WriteTimeout(t time.Duration) ServerOption {
	return func(c *serverConfig) error {
		c.apiWriteTimeout = t
		return nil
	}
}

// ReadTimeout config for API
func ReadTimeout(t time.Duration) ServerOption {
	return func(c *serverConfig) error {
		c.apiReadTimeout = t
		return nil
	}
}
