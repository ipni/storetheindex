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

// MainMode controls how the Main mirror backend is used.
type MainMode string

const (
	// MainModeUnspecified disables Main read and write.
	MainModeUnspecified MainMode = ""
	// MainModeNone is an explicit alias for disabled Main mirror.
	MainModeNone MainMode = "none"
	// MainModeRead enables reading from Main.
	MainModeRead MainMode = "read"
	// MainModeWrite enables writing to Main.
	MainModeWrite MainMode = "write"
	// MainModeReadWrite enables reading from and writing to Main.
	MainModeReadWrite MainMode = "readwrite"
)

// CanRead reports whether MainMode enables Main reads.
func (m MainMode) CanRead() bool {
	switch m {
	case MainModeRead, MainModeReadWrite:
		return true
	default:
		return false
	}
}

// CanWrite reports whether MainMode enables Main writes.
func (m MainMode) CanWrite() bool {
	switch m {
	case MainModeWrite, MainModeReadWrite:
		return true
	default:
		return false
	}
}

// Enabled reports whether Main read and/or write is enabled.
func (m MainMode) Enabled() bool {
	return m.CanRead() || m.CanWrite()
}

// Valid reports whether m is a recognized MainMode value.
func (m MainMode) Valid() bool {
	switch m {
	case MainModeUnspecified, MainModeNone, MainModeRead, MainModeWrite, MainModeReadWrite:
		return true
	default:
		return false
	}
}

// StoreConfig wraps a filestore backend with optional CAR file compression.
type StoreConfig struct {
	filestore.Config
	// Compress specifies how to compress files. One of: "gzip", "none".
	// Defaults to "gzip" if unspecified.
	Compress string
}

// Mirror configures if, how, and where to store content advertisements data in
// CAR files. The mirror may be readable, writable, both, or neither. If Main
// is unused and External is unset, or a storage type is not specified, then the
// mirror is not used.
type Mirror struct {
	// MainMode controls Main mirror access: "", "none", "read", "write", or
	// "readwrite" (lowercase). It does not affect External.
	MainMode MainMode

	// Main configures the owned file store for mirror read and write
	// operations. Controlled by MainMode.
	Main StoreConfig

	// External configures independent file stores for mirror read operations.
	// When set, all entries are raced in parallel after a Main miss (or as the
	// sole sources when Main read is disabled). The first successful retrieval
	// wins; 404s and errors are misses. Not gated by MainMode.
	External []StoreConfig
}

// NewMirror returns Mirror with values set to their defaults.
func NewMirror() Mirror {
	return Mirror{
		MainMode: MainModeNone,
		Main: StoreConfig{
			Compress: "gzip",
			Config: filestore.Config{
				Local: filestore.LocalConfig{
					DefaultPathSplit: []int{11, 2},
				},
			},
		},
		External: []StoreConfig{},
	}
}

// PopulateUnset replaces zero-values in the config with default values.
func (c *Mirror) PopulateUnset() {
	def := NewMirror()
	if c.Main.Compress == "" {
		c.Main.Compress = def.Main.Compress
	}
	if c.External == nil {
		c.External = []StoreConfig{}
	}
	for i := range c.External {
		if filestoreConfigured(&c.External[i].Config) && c.External[i].Compress == "" {
			c.External[i].Compress = def.Main.Compress
		}
	}
}

// UnmarshalJSON loads Mirror config, accepting legacy Read/Write,
// Retrieval/Storage, and top-level Compress fields and converting them to
// MainMode/Main/External with per-store Compress.
// FallbackRetrieval from unreleased configs is ignored.
func (c *Mirror) UnmarshalJSON(data []byte) error {
	aux := mirrorJSON{mirrorPlain: (*mirrorPlain)(c)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if err := c.convertLegacy(aux); err != nil {
		return err
	}
	if !c.MainMode.Valid() {
		return fmt.Errorf("invalid AdvertisementMirror.MainMode %q; want one of: %q, %q, %q, %q, %q",
			c.MainMode, MainModeUnspecified, MainModeNone, MainModeRead, MainModeWrite, MainModeReadWrite)
	}
	return nil
}

// mirrorPlain is Mirror without methods, so embedding it in mirrorJSON does not
// recurse into UnmarshalJSON.
type mirrorPlain Mirror

// mirrorJSON is used only for unmarshaling: it embeds Mirror fields and carries
// legacy Read/Write, Storage/Retrieval, and top-level Compress for convertLegacy.
type mirrorJSON struct {
	*mirrorPlain
	Compress  string            `json:"Compress,omitempty"`
	Read      bool              `json:"Read,omitempty"`
	Write     bool              `json:"Write,omitempty"`
	Retrieval *filestore.Config `json:"Retrieval,omitempty"`
	Storage   *filestore.Config `json:"Storage,omitempty"`
}

// convertLegacy converts deprecated Read/Write, Retrieval/Storage, and
// top-level Compress into MainMode/Main/External with per-store Compress.
// Read/Write filter which backends are active; the remaining configured
// backends then determine MainMode/Main/External.
func (c *Mirror) convertLegacy(aux mirrorJSON) error {
	hasLegacyBackends := filestoreConfigured(aux.Storage) || filestoreConfigured(aux.Retrieval)
	hasNewBackends := filestoreConfigured(&c.Main.Config) || len(c.External) > 0
	hasLegacyMode := aux.Read || aux.Write
	hasNewMode := c.MainMode.Enabled()

	if (hasLegacyMode || hasLegacyBackends) && (hasNewMode || hasNewBackends) {
		return errors.New("advertisement mirror config mixes legacy Read/Write/Storage/Retrieval with MainMode/Main/External; use only the new fields")
	}

	anyExternalCompressSet := false
	for i := range c.External {
		if c.External[i].Compress != "" {
			anyExternalCompressSet = true
			break
		}
	}

	if aux.Compress != "" && (c.Main.Compress != "" || anyExternalCompressSet) {
		return errors.New("advertisement mirror config mixes legacy top-level Compress with Main/External.Compress; use only per-store Compress")
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
			c.MainMode = MainModeReadWrite
			c.Main = StoreConfig{Config: *storage}
			log.Warn("converted legacy AdvertisementMirror Storage/Retrieval (identical) to MainMode/Main; please update config to use MainMode/Main/External")
		} else {
			// Distinct backends: Storage is owned Main (write-only).
			// Retrieval becomes External and covers reads by its presence, so
			// MainMode is write even when legacy Read was also set.
			c.MainMode = MainModeWrite
			c.Main = StoreConfig{Config: *storage}
			c.External = []StoreConfig{{Config: *retrieval}}
			log.Warn("converted legacy AdvertisementMirror Storage to Main (write) and Retrieval to External; please update config to use MainMode/Main/External")
		}

	case filestoreConfigured(storage):
		// Write backend only
		c.MainMode = MainModeWrite
		c.Main = StoreConfig{Config: *storage}
		log.Warn("converted legacy AdvertisementMirror Storage to Main (write); please update config to use MainMode/Main/External")

	case filestoreConfigured(retrieval):
		// Read backend only
		c.MainMode = MainModeRead
		c.Main = StoreConfig{Config: *retrieval}
		log.Warn("converted legacy AdvertisementMirror Retrieval to Main (read); please update config to use MainMode/Main/External")
	}

	if aux.Compress != "" {
		c.Main.Compress = aux.Compress
		for i := range c.External {
			if filestoreConfigured(&c.External[i].Config) {
				c.External[i].Compress = aux.Compress
			}
		}
		log.Warn("converted legacy AdvertisementMirror.Compress to Main/External.Compress; please update config to set Compress on each store")
	}

	return nil
}

func filestoreConfigured(cfg *filestore.Config) bool {
	return cfg != nil && cfg.Type != "" && cfg.Type != "none"
}
