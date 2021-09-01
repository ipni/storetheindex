package ingestion

import (
	"fmt"
	"time"
)

const (
	defaultSyncTimeout = 5 * time.Minute
)

// SyncConfig holds the configured options after applying a number of
// SyncOption funcs.
//
// This type should not be used directly by end users; it's only exposed as a
// side effect of SyncOption
type SyncConfig struct {
	SyncTimeout time.Duration
}

// SyncOption to config syncing process
type SyncOption func(*SyncConfig) error

// SyncDefaults are the default options. This option will be automatically
// prepended to any options you pass to the constructor.
var SyncDefaults = func(o *SyncConfig) error {
	o.SyncTimeout = defaultSyncTimeout
	return nil
}

// Apply applies the given options to this Option
func (c *SyncConfig) Apply(opts ...SyncOption) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("httpserver option %d failed: %s", i, err)
		}
	}
	return nil
}

// SyncTimeout for syncing process
func SyncTimeout(t time.Duration) SyncOption {
	return func(c *SyncConfig) error {
		c.SyncTimeout = t
		return nil
	}
}
