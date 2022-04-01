package httpfinderserver

import (
	"fmt"
	"time"
)

const (
	apiWriteTimeout = 30 * time.Second
	apiReadTimeout  = 30 * time.Second
	cacheMaxAge     = 30 * time.Minute
	maxConns        = 8_000
)

// serverConfig is a structure containing all the options that can be used when constructing an http server
type serverConfig struct {
	apiWriteTimeout time.Duration
	apiReadTimeout  time.Duration
	cacheMaxAge     time.Duration
	maxConns        int
}

// ServerOption for httpserver
type ServerOption func(*serverConfig) error

// defaults are the default ptions. This option will be automatically
// prepended to any options you pass to the constructor.
var serverDefaults = func(o *serverConfig) error {
	o.apiWriteTimeout = apiWriteTimeout
	o.apiReadTimeout = apiReadTimeout
	o.cacheMaxAge = cacheMaxAge
	o.maxConns = maxConns
	return nil
}

// apply applies the given options to this config
func (c *serverConfig) apply(opts ...ServerOption) error {
	err := serverDefaults(c)
	if err != nil {
		// Failure of default option should panic
		panic("default option failed: " + err.Error())
	}
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
