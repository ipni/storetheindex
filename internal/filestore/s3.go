package filestore

import (
	"context"
	"errors"
	"io"

	"github.com/ipni/storetheindex/config"
)

var ErrNotImplemented = errors.New("not implemented")

// S3 is a file store that stores files in AWS S3.
type S3 struct {
	basePath   string
	bucketName string
	region     string
	accessKey  string
	secretKey  string
}

func newS3(cfg config.S3FileStore) (*S3, error) {
	if cfg.BucketName == "" {
		return nil, errors.New("s3 configuration missing 'BucketName'")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3 configuration missing 'Region'")
	}

	return nil, ErrNotImplemented
}

func (s3 *S3) Delete(ctx context.Context, path string) error {
	return ErrNotImplemented
}

func (s3 *S3) Get(ctx context.Context, path string) (*File, io.ReadCloser, error) {
	return nil, nil, ErrNotImplemented
}

func (s3 *S3) Head(ctx context.Context, path string) (*File, error) {
	return nil, ErrNotImplemented
}

func (s3 *S3) List(ctx context.Context, path string, recursive bool) (<-chan *File, <-chan error) {
	c := make(chan *File)
	e := make(chan error, 1)

	close(c)
	e <- ErrNotImplemented
	close(e)

	return c, e
}

func (s3 *S3) Put(ctx context.Context, path string, reader io.Reader) (*File, error) {
	return nil, ErrNotImplemented
}

func (s3 *S3) Type() string {
	return "s3"
}
