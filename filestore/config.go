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
}

type LocalConfig struct {
	// BasePath is the filesystem directory where files are stored.
	BasePath string
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

// MakeFilestore creates a new storage system of the configured type.
func MakeFilestore(cfg Config) (Interface, error) {
	switch cfg.Type {
	case "local":
		return NewLocal(cfg.Local.BasePath)
	case "s3":
		return NewS3(cfg.S3.BucketName,
			WithEndpoint(cfg.S3.Endpoint),
			WithRegion(cfg.S3.Region),
			WithKeys(cfg.S3.AccessKey, cfg.S3.SecretKey),
		)
	case "":
		return nil, errors.New("storage type not defined")
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported file storage type: %s", cfg.Type)
}
