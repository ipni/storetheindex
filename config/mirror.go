package config

import (
	"github.com/ipni/storetheindex/filestore"
)

// Mirror configures if, how, and where to store content advertisements data in
// CAR files. The mirror may be readable, writable, both, or neither. If the
// mirror is neither readable or writable, or a storage type is not specified,
// then the mirror is not used.
type Mirror struct {
	// Read specifies to read advertisement content from the mirror.
	Read bool
	// Write specifies to write advertisement content to the mirror.
	Write bool
	// Compress specifies how to compress files. One of: "gzip", "none".
	// Defaults to "gzip" if unspecified.
	Compress string
	// Storage configures the backing file store for the mirror.
	Storage filestore.Config
}

// NewMirror returns Mirror with values set to their defaults.
func NewMirror() Mirror {
	return Mirror{
		Compress: "gzip",
	}
}

// PopulateUnset replaces zero-values in the config with default values.
func (c *Mirror) PopulateUnset() {
	def := NewMirror()
	if c.Compress == "" {
		c.Compress = def.Compress
	}
}
