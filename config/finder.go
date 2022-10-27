package config

import (
	"time"
)

type Finder struct {
	// ApiReadTimeout sets the HTTP server's maximum duration for reading the
	// entire request, including the body. A value of zero sets the default,
	// and a negative value means there will be no timeout.
	ApiReadTimeout Duration
	// ApiWriteTimeout sets the HTTP server's maximum duration before timing
	// out writes of the response. A value of zero sets the default and a
	// negative value means there will be no timeout.
	ApiWriteTimeout Duration
	// MaxConnections is maximum number of simultaneous connections that the
	// HTTP server will accept. A value of zero sets the default and a negative
	// value means there is no limit.
	MaxConnections int
	// Webpage is a domain to display when the homepage of the finder is
	// accessed over HTTP.
	Webpage string
}

func NewFinder() Finder {
	return Finder{
		ApiReadTimeout:  Duration(30 * time.Second),
		ApiWriteTimeout: Duration(30 * time.Second),
		MaxConnections:  8_000,
		Webpage:         "https://web.cid.contact/",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (f *Finder) populateUnset() {
	def := NewFinder()
	if f.ApiReadTimeout == 0 {
		f.ApiReadTimeout = def.ApiReadTimeout
	}
	if f.ApiWriteTimeout == 0 {
		f.ApiWriteTimeout = def.ApiWriteTimeout
	}
	if f.MaxConnections == 0 {
		f.MaxConnections = def.MaxConnections
	}
	if f.Webpage == "" {
		f.Webpage = def.Webpage
	}
}
