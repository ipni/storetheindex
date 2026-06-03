package filestore

import (
	"errors"
	"fmt"
)

// Config configures a particular file store implementation.
type Config struct {
	// Type is the type of file store to use: "", "local", "s3"
	Type string
	// Local configures storing files in local filesystem.
	Local LocalConfig
	// S3 configures storing files in S3.
	S3 S3Config
	// HTTP configures storing files in HTTP.
	HTTP HTTPConfig
}

type LocalConfig struct {
	// BasePath is the filesystem directory where files are stored.
	BasePath string
	// DefaultPathSplit determines how the car name is split into subdirectories
	DefaultPathSplit []int
}

type S3Config struct {
	BucketName string

	// ## Optional Overrides ##
	//
	// These values are generally set by the environment and should only be
	// provided when necessary to override values from the environment, or when
	// the environment is not configured.
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
}

type HTTPConfig struct {
	// BaseURL is the base URL of the HTTP file store.
	BaseURL string
}

// MakeFilestore creates a new storage system of the configured type.
func MakeFilestore(cfg Config) (Interface, error) {
	switch cfg.Type {
	case "local":
		return NewLocal(cfg.Local.BasePath,
			WithDefaultPathSplit(cfg.Local.DefaultPathSplit...),
		)
	case "s3":
		return NewS3(cfg.S3.BucketName,
			WithEndpoint(cfg.S3.Endpoint),
			WithRegion(cfg.S3.Region),
			WithKeys(cfg.S3.AccessKey, cfg.S3.SecretKey),
		)
	case "http":
		return NewHTTP(cfg.HTTP.BaseURL)
	case "":
		return nil, errors.New("storage type not defined")
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported file storage type: %s", cfg.Type)
}
