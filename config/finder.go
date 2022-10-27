package config

import (
	"time"
)

type Finder struct {
	// api write timeout
	ApiWriteTimeout Duration
	// api read timeout
	ApiReadTimeout Duration
	// maximum number of connections
	MaxConnections int
}

func NewFinder() Finder {
	return Finder{
		ApiWriteTimeout: Duration(30 * time.Second),
		ApiReadTimeout:  Duration(30 * time.Second),
		MaxConnections:  8_000,
	}
}

// populateUnset replaces zero-values in the config with default values.
func (f *Finder) populateUnset() {
	def := NewFinder()
	if f.ApiWriteTimeout == 0 {
		f.ApiWriteTimeout = def.ApiWriteTimeout
	}
	if f.ApiReadTimeout == 0 {
		f.ApiReadTimeout = def.ApiReadTimeout
	}
	if f.MaxConnections == 0 {
		f.MaxConnections = def.MaxConnections
	}
}
