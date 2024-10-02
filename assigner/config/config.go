package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	sticfg "github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
)

// Config is used to load config files.
type Config struct {
	Version    int              // config version.
	Identity   sticfg.Identity  // peer identity.
	Assignment Assignment       // Indexer assignment settings.
	Bootstrap  sticfg.Bootstrap // Peers to connect to for gossip,
	Daemon     Daemon           // daemon settings.
	Logging    Logging          // logging configuration.,
	Peering    sticfg.Peering   // peering service configuration.
}

const (
	// DefaultPathName is the default config dir name.
	DefaultPathName = ".assigner"
	// DefaultPathRoot is the path to the default config dir location.
	DefaultPathRoot = "~/" + DefaultPathName
	// DefaultConfigFile is the filename of the configuration file.
	DefaultConfigFile = "config"
	// EnvDir is the environment variable used to change the path root.
	EnvDir = "ASSIGNER_PATH"

	Version = 1
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
			err = sticfg.ErrNotInitialized
		}
		return nil, err
	}
	defer f.Close()

	// Populate with initial values in case they are not present in config.
	cfg := Config{
		Assignment: NewAssignment(),
		Bootstrap:  sticfg.NewBootstrap(),
		Daemon:     NewDaemon(),
		Logging:    NewLogging(),
		Peering:    sticfg.NewPeering(),
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
	c.Assignment.populateUnset()
	c.Daemon.populateUnset()
	c.Logging.populateUnset()
}
