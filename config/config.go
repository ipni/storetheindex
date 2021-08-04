package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
)

// Config is used to load storetheindex config files
type Config struct {
	Identity  Identity  // peer identity
	Datastore Datastore // non-index data storage
	Addresses Addresses // addresses to listen on
	Discovery Discovery // provider discovery configuration
	Indexer   Indexer   // indexer code configuration
}

const (
	// DefaultPathName is the default config dir name
	DefaultPathName = ".storetheindex"
	// DefaultPathRoot is the path to the default config dir location.
	DefaultPathRoot = "~/" + DefaultPathName
	// DefaultConfigFile is the filename of the configuration file
	DefaultConfigFile = "config"
	// EnvDir is the environment variable used to change the path root.
	EnvDir = "STORETHEINDEX_PATH"
)

var (
	ErrInitialized    = errors.New("already initialized")
	ErrNotInitialized = errors.New("not initialized")
)

// Filename returns the configuration file path given a configuration root
// directory. If the configuration root directory is empty, use the default one
func Filename(configRoot string) (string, error) {
	return Path(configRoot, DefaultConfigFile)
}

// Marshal configuration with JSON
func Marshal(value interface{}) ([]byte, error) {
	// need to prettyprint, hence MarshalIndent, instead of Encoder
	return json.MarshalIndent(value, "", "  ")
}

// Path returns the config file path relative to the configuration root. If an
// empty string is provided for `configRoot`, the default root is used.
func Path(configRoot, configFile string) (string, error) {
	if configRoot == "" {
		var err error
		configRoot, err = PathRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(configRoot, configFile), nil
}

// PathRoot returns the default configuration root directory
func PathRoot() (string, error) {
	dir := os.Getenv(EnvDir)
	if dir != "" {
		return dir, nil
	}
	return homedir.Expand(DefaultPathRoot)
}

// Load reads the json-serialized config at the specified path
func Load(filename string) (*Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotInitialized
		}
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failure to decode config: %s", err)
	}
	return &cfg, err
}

// Save writes the json-serialized config to the specified path
func (c *Config) Save(filename string) error {
	err := os.MkdirAll(filepath.Dir(filename), 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(filename)
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
