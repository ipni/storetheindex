package filestore

import (
	"fmt"
)

type s3Config struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	pageSize  int
}

type S3Option func(*s3Config) error

func getS3Opts(opts []S3Option) (s3Config, error) {
	var cfg s3Config
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return s3Config{}, fmt.Errorf("option %d error: %w", i, err)
		}
	}
	return cfg, nil
}

func WithEndpoint(ep string) S3Option {
	return func(c *s3Config) error {
		c.endpoint = ep
		return nil
	}
}

func WithRegion(region string) S3Option {
	return func(c *s3Config) error {
		c.region = region
		return nil
	}
}

func WithKeys(accessKey, secretKey string) S3Option {
	return func(c *s3Config) error {
		c.accessKey = accessKey
		c.secretKey = secretKey
		return nil
	}
}

func WithPageSize(pageSize int) S3Option {
	return func(c *s3Config) error {
		c.pageSize = pageSize
		return nil
	}
}

type localConfig struct {
	basePath  string
	pathSplit []int
}

type LocalOption func(*localConfig) error

func getLocalOpts(opts []LocalOption) (localConfig, error) {
	var cfg localConfig
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return localConfig{}, fmt.Errorf("option %d error: %w", i, err)
		}
	}
	return cfg, nil
}

func WithDefaultPathSplit(defaultPathSplit ...int) LocalOption {
	for i, splitSegment := range defaultPathSplit {
		if splitSegment <= 0 {
			return func(lc *localConfig) error {
				return fmt.Errorf(
					"invalid path split config for segment %d of size %d: must be a positive integer",
					i, splitSegment,
				)
			}
		}
	}

	return func(lc *localConfig) error {
		lc.pathSplit = defaultPathSplit
		return nil
	}
}
