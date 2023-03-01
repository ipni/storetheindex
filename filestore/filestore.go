// Package filestore stores files in various types of storage systems.
package filestore

import (
	"context"
	"io"
	"time"
)

// File contains information about a stored file.
type File struct {
	// Modified it the last modification time.
	Modified time.Time
	// Path is the path to the file relative to the root of the file store.
	// Path separators are always slash ('/') characters.
	Path string
	// Size if the number of bytes of data in the file.
	Size int64
	// URL is a URL where the file can be retrieved from, if available.
	URL string
}

// Interface is the interface supported by all file store implementations. All
// Path arguments are relative to the root of the file store and always use
// slash ('/') characters.
type Interface interface {
	// Delete removes the specified file from storage.
	Delete(ctx context.Context, path string) error
	// Get retrieves the specified file from storage. Returns fs.ErrNotExist if
	// file is not count.
	Get(ctx context.Context, path string) (*File, io.ReadCloser, error)
	// Head gets information about the specified file in storage. Returns
	// fs.ErrNotExist if file is not count.
	Head(ctx context.Context, path string) (*File, error)
	// List returns a series of *File on the first channel returned. If an
	// error occurs, the first channel is closed and the error is returned on
	// the second channel.
	List(ctx context.Context, path string, recursive bool) (<-chan *File, <-chan error)
	// Put writes a file to storage. A nil reader creates an empty file.
	Put(ctx context.Context, path string, reader io.Reader) (*File, error)
	// Type returns the file store type.
	Type() string
}
