package httpsender

import (
	"fmt"
	"net/http"
	"time"
)

const defaultTimeout = time.Minute

type config struct {
	timeout time.Duration
	client  *http.Client
}

type Option func(*config) error

// apply applies options to this config
func (c *config) apply(opts []Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

// WithTimeout configures the timeout to wait for a response.
func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) error {
		cfg.timeout = timeout
		return nil
	}
}

// WithClient uses an existing http.Client with the Sender.
func WithClient(c *http.Client) Option {
	return func(cfg *config) error {
		cfg.client = c
		return nil
	}
}
