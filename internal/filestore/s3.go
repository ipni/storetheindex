package filestore

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"

	"github.com/ipni/storetheindex/config"

	// AWS
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3 is a file store that stores files in AWS S3.
//
// The region is set by environment variable and authentication is done by
// assuming a role, which is handled by infrastructure.
type S3 struct {
	bucketName string
	client     *s3.Client
	uploader   *manager.Uploader
}

func newS3(cfg config.S3FileStore) (*S3, error) {
	if cfg.BucketName == "" {
		return nil, errors.New("s3 configuration missing 'BucketName'")
	}

	var usePathStyle bool
	var cfgOpts []func(*awsconfig.LoadOptions) error

	if cfg.Region != "" {
		cfgOpts = append(cfgOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Endpoint != "" {
		var epResolverFunc aws.EndpointResolverWithOptionsFunc
		epResolverFunc = aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: cfg.Endpoint}, nil
			})
		usePathStyle = true
		cfgOpts = append(cfgOpts, awsconfig.WithEndpointResolverWithOptions(epResolverFunc))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		staticCreds := credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     cfg.AccessKey,
				SecretAccessKey: cfg.SecretKey,
				Source:          "filestore configuration",
			},
		}
		cfgOpts = append(cfgOpts, awsconfig.WithCredentialsProvider(staticCreds))
	}

	awscfg, err := awsconfig.LoadDefaultConfig(context.Background(), cfgOpts...)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	})
	return &S3{
		bucketName: cfg.BucketName,
		client:     client,
		uploader:   manager.NewUploader(client),
	}, nil
}

func (s *S3) Delete(ctx context.Context, relPath string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(relPath),
	})

	return err
}

func (s *S3) Get(ctx context.Context, relPath string) (*File, io.ReadCloser, error) {
	rsp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(relPath),
	})

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil, ErrNotFound
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if "NotFound" == apiErr.ErrorCode() {
				return nil, nil, ErrNotFound
			}
		}
		return nil, nil, err
	}

	file := &File{
		Path: relPath,
		Size: rsp.ContentLength,
	}
	if rsp.LastModified != nil {
		file.Modified = *rsp.LastModified
	}

	return file, &wrappedReadCloser{rsp.Body}, nil
}

func (s *S3) Head(ctx context.Context, relPath string) (*File, error) {
	rsp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(relPath),
	})

	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if "NotFound" == apiErr.ErrorCode() {
				return nil, ErrNotFound
			}
		}
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	file := &File{
		Path: relPath,
		Size: rsp.ContentLength,
	}
	if rsp.LastModified != nil {
		file.Modified = *rsp.LastModified
	}

	return file, nil
}

func (s *S3) List(ctx context.Context, relPath string, recursive bool) (<-chan *File, <-chan error) {
	fc := make(chan *File)
	ec := make(chan error, 1)

	go func() {
		defer close(fc)
		defer close(ec)
		req := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucketName),
			Prefix: aws.String(relPath),
		}

		for {
			rsp, err := s.client.ListObjectsV2(ctx, req)
			if err != nil {
				ec <- err
				return
			}

			for _, content := range rsp.Contents {
				if strings.HasSuffix(*content.Key, "/") {
					continue
				}

				if !recursive {
					// If not resursive then skip subdirectories of relPath.
					dir, _ := path.Split(strings.TrimPrefix(*content.Key, relPath))
					if dir != "" {
						continue
					}
				}

				file := &File{
					Path: *content.Key,
					Size: content.Size,
				}
				if content.LastModified != nil {
					file.Modified = *content.LastModified
				}

				select {
				case fc <- file:
				case <-ctx.Done():
					ec <- ctx.Err()
					return
				}
			}

			if !rsp.IsTruncated {
				break
			}

			req.ContinuationToken = rsp.NextContinuationToken
		}
	}()

	return fc, ec
}

func (s *S3) Put(ctx context.Context, relPath string, reader io.Reader) (*File, error) {
	rsp, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(relPath),
		Body:        reader,
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return nil, err
	}

	file, err := s.Head(ctx, relPath)
	if err != nil {
		return nil, err
	}
	file.URL = rsp.Location

	return file, nil
}

func (s *S3) Type() string {
	return "s3"
}

// wrappedReadCloser wraps an io.ReadCloser to ensure that Read returns io.EOF
// only on the call after all data has been read, that is when n == 0.
type wrappedReadCloser struct {
	r io.ReadCloser
}

func (w wrappedReadCloser) Read(p []byte) (int, error) {
	n, err := w.r.Read(p)
	if err != nil && errors.Is(err, io.EOF) && n != 0 {
		return n, nil
	}
	return n, err
}

func (w wrappedReadCloser) Close() error {
	return w.r.Close()
}
