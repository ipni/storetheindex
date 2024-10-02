package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipni/storetheindex/fsutil"
)

// Config is used to load config files.
type Config struct {
	Version   int       // config version
	Identity  Identity  // peer identity
	Addresses Addresses // addresses to listen on
	Bootstrap Bootstrap // Peers to connect to for gossip
	Datastore Datastore // datastore config
	Discovery Discovery // provider pubsub peers
	Finder    Finder    // finder code configuration
	Indexer   Indexer   // indexer code configuration
	Ingest    Ingest    // ingestion related configuration.
	Logging   Logging   // logging configuration.
	Peering   Peering   // peering service configuration.
}

const (
	// DefaultPathName is the default config dir name.
	DefaultPathName = ".storetheindex"
	// DefaultPathRoot is the path to the default config dir location.
	DefaultPathRoot = "~/" + DefaultPathName
	// DefaultConfigFile is the filename of the configuration file.
	DefaultConfigFile = "config"
	// EnvDir is the environment variable used to change the path root.
	EnvDir = "STORETHEINDEX_PATH"

	Version = 2
)

var (
	ErrInitialized    = errors.New("configuration file already exists")
	ErrNotInitialized = errors.New("not initialized")
)

// Marshal configuration with JSON.
func Marshal(value interface{}) ([]byte, error) {
	// need to prettyprint, hence MarshalIndent, instead of Encoder.
	return json.MarshalIndent(value, "", "  ")
}

// Path returns the config file path relative to the configuration root. If an
// empty string is provided for `configRoot`, the default root is used. If
// configFile is an absolute path, then configRoot is ignored.
func Path(configRoot, configFile string) (string, error) {
	if configFile == "" {
		configFile = DefaultConfigFile
	} else if filepath.IsAbs(configFile) {
		return filepath.Clean(configFile), nil
	}
	if configRoot == "" {
		var err error
		configRoot, err = PathRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(configRoot, configFile), nil
}

// PathRoot returns the default configuration root directory.
func PathRoot() (string, error) {
	dir := os.Getenv(EnvDir)
	if dir != "" {
		return dir, nil
	}
	return fsutil.ExpandHome(DefaultPathRoot)
}

// Load reads the json-serialized config at the specified path.
func Load(filePath string) (*Config, error) {
	var err error
	if filePath == "" {
		filePath, err = Path("", "")
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotInitialized
		}
		return nil, err
	}
	defer f.Close()

	// Populate with initial values in case they are not present in config.
	cfg := Config{
		Addresses: NewAddresses(),
		Bootstrap: NewBootstrap(),
		Datastore: NewDatastore(),
		Discovery: NewDiscovery(),
		Finder:    NewFinder(),
		Indexer:   NewIndexer(),
		Ingest:    NewIngest(),
		Logging:   NewLogging(),
		Peering:   NewPeering(),
	}

	if err = json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, err
	}

	cfg.populateUnset()

	return &cfg, nil
}

// UpgradeConfig upgrades (or downgrades) the config file to the current
// version. If the config file is at the current version a backup is still
// created and the config rewritten with any unconfigured values set to their
// defaults.
func (c *Config) UpgradeConfig(filePath string) error {
	prevName := fmt.Sprintf("%s.v%d", filePath, c.Version)
	err := os.Rename(filePath, prevName)
	if err != nil {
		return err
	}
	c.Version = Version
	err = c.Save(filePath)
	if err != nil {
		return err
	}
	return nil
}

// Save writes the json-serialized config to the specified path.
func (c *Config) Save(filePath string) error {
	var err error
	if filePath == "" {
		filePath, err = Path("", "")
		if err != nil {
			return err
		}
	}

	err = os.MkdirAll(filepath.Dir(filePath), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	buf, err := Marshal(c)
	if err != nil {
		return err
	}
	_, err = f.Write(buf)
	return err
}

// String returns a pretty-printed json config.
func (c *Config) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (c *Config) populateUnset() {
	c.Addresses.populateUnset()
	c.Datastore.populateUnset()
	c.Discovery.populateUnset()
	c.Finder.populateUnset()
	c.Indexer.populateUnset()
	c.Ingest.populateUnset()
	c.Logging.populateUnset()
}
