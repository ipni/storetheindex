package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/filestore"
)

var log = logging.Logger("indexer/config")

// LocalMode controls how the Local mirror backend is used.
type LocalMode string

const (
	// LocalModeUnspecified disables Local read and write.
	LocalModeUnspecified LocalMode = ""
	// LocalModeNone is an explicit alias for disabled Local mirror.
	LocalModeNone LocalMode = "none"
	// LocalModeRead enables reading from Local.
	LocalModeRead LocalMode = "read"
	// LocalModeWrite enables writing to Local.
	LocalModeWrite LocalMode = "write"
	// LocalModeReadWrite enables reading from and writing to Local.
	LocalModeReadWrite LocalMode = "readwrite"
)

// CanRead reports whether LocalMode enables Local reads.
func (m LocalMode) CanRead() bool {
	switch m {
	case LocalModeRead, LocalModeReadWrite:
		return true
	default:
		return false
	}
}

// CanWrite reports whether LocalMode enables Local writes.
func (m LocalMode) CanWrite() bool {
	switch m {
	case LocalModeWrite, LocalModeReadWrite:
		return true
	default:
		return false
	}
}

// Enabled reports whether Local read and/or write is enabled.
func (m LocalMode) Enabled() bool {
	return m.CanRead() || m.CanWrite()
}

// Valid reports whether m is a recognized LocalMode value.
func (m LocalMode) Valid() bool {
	switch m {
	case LocalModeUnspecified, LocalModeNone, LocalModeRead, LocalModeWrite, LocalModeReadWrite:
		return true
	default:
		return false
	}
}

// Mirror configures if, how, and where to store content advertisements data in
// CAR files. The mirror may be readable, writable, both, or neither. If Local
// is unused and External is unset, or a storage type is not specified, then the
// mirror is not used.
type Mirror struct {
	// Compress specifies how to compress files. One of: "gzip", "none".
	// Defaults to "gzip" if unspecified.
	Compress string

	// LocalMode controls Local mirror access: "", "none", "read", "write", or
	// "readwrite" (lowercase). It does not affect External.
	LocalMode LocalMode

	// Local configures the owned file store for mirror read and write
	// operations. Controlled by LocalMode.
	Local filestore.Config

	// External configures an independent file store for mirror read operations.
	// When set, it is always used for reads: as the sole source when Local read
	// is disabled, or as a fallback when Local read misses. Not gated by
	// LocalMode.
	External filestore.Config
}

// NewMirror returns Mirror with values set to their defaults.
func NewMirror() Mirror {
	return Mirror{
		Compress:  "gzip",
		LocalMode: LocalModeNone,
		Local: filestore.Config{
			Local: filestore.LocalConfig{
				DefaultPathSplit: []int{11, 2},
			},
		},
	}
}

// PopulateUnset replaces zero-values in the config with default values.
func (c *Mirror) PopulateUnset() {
	def := NewMirror()
	if c.Compress == "" {
		c.Compress = def.Compress
	}
}

// UnmarshalJSON loads Mirror config, accepting legacy Read/Write and
// Retrieval/Storage fields and converting them to LocalMode/Local/External.
// FallbackRetrieval from unreleased configs is ignored.
func (c *Mirror) UnmarshalJSON(data []byte) error {
	aux := mirrorJSON{mirrorPlain: (*mirrorPlain)(c)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if err := c.convertLegacy(aux); err != nil {
		return err
	}
	if !c.LocalMode.Valid() {
		return fmt.Errorf("invalid AdvertisementMirror.LocalMode %q; want one of: %q, %q, %q, %q, %q",
			c.LocalMode, LocalModeUnspecified, LocalModeNone, LocalModeRead, LocalModeWrite, LocalModeReadWrite)
	}
	return nil
}

// mirrorPlain is Mirror without methods, so embedding it in mirrorJSON does not
// recurse into UnmarshalJSON.
type mirrorPlain Mirror

// mirrorJSON is used only for unmarshaling: it embeds Mirror fields and carries
// legacy Read/Write and Storage/Retrieval for convertLegacy.
type mirrorJSON struct {
	*mirrorPlain
	Read      bool              `json:"Read,omitempty"`
	Write     bool              `json:"Write,omitempty"`
	Retrieval *filestore.Config `json:"Retrieval,omitempty"`
	Storage   *filestore.Config `json:"Storage,omitempty"`
}

// convertLegacy converts deprecated Read/Write and Retrieval/Storage into
// LocalMode/Local/External. Read/Write filter which backends are active; the
// remaining configured backends then determine LocalMode/Local/External.
func (c *Mirror) convertLegacy(aux mirrorJSON) error {
	hasLegacyBackends := filestoreConfigured(aux.Storage) || filestoreConfigured(aux.Retrieval)
	hasNewBackends := filestoreConfigured(&c.Local) || filestoreConfigured(&c.External)
	hasLegacyMode := aux.Read || aux.Write
	hasNewMode := c.LocalMode.Enabled()

	if (hasLegacyMode || hasLegacyBackends) && (hasNewMode || hasNewBackends) {
		return errors.New("advertisement mirror config mixes legacy Read/Write/Storage/Retrieval with LocalMode/Local/External; use only the new fields")
	}

	storage, retrieval := aux.Storage, aux.Retrieval

	if !aux.Read {
		retrieval = nil
	}

	if !aux.Write {
		storage = nil
	}

	switch {
	case filestoreConfigured(storage) && filestoreConfigured(retrieval):
		if reflect.DeepEqual(*storage, *retrieval) {
			// Storage and retrieval are identical:
			c.LocalMode = LocalModeReadWrite
			c.Local = *storage
			log.Warn("converted legacy AdvertisementMirror Storage/Retrieval (identical) to LocalMode/Local; please update config to use LocalMode/Local/External")
		} else {
			// Distinct backends: Storage is owned Local (write-only).
			// Retrieval becomes External and covers reads by its presence, so
			// LocalMode is write even when legacy Read was also set.
			c.LocalMode = LocalModeWrite
			c.Local = *storage
			c.External = *retrieval
			log.Warn("converted legacy AdvertisementMirror Storage to Local (write) and Retrieval to External; please update config to use LocalMode/Local/External")
		}

	case filestoreConfigured(storage):
		// Write backend only
		c.LocalMode = LocalModeWrite
		c.Local = *storage
		log.Warn("converted legacy AdvertisementMirror Storage to Local (write); please update config to use LocalMode/Local/External")

	case filestoreConfigured(retrieval):
		// Read backend only
		c.LocalMode = LocalModeRead
		c.Local = *retrieval
		log.Warn("converted legacy AdvertisementMirror Retrieval to Local (read); please update config to use LocalMode/Local/External")
	}

	return nil
}

func filestoreConfigured(cfg *filestore.Config) bool {
	return cfg != nil && cfg.Type != "" && cfg.Type != "none"
}
