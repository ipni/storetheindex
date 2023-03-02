package filestore

import (
	"fmt"
)

type s3Config struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
}

type S3Option func(*s3Config) error

func getOpts(opts []S3Option) (s3Config, error) {
	var cfg s3Config
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return s3Config{}, fmt.Errorf("option %d error: %s", i, err)
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
