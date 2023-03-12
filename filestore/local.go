package filestore

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/ipni/storetheindex/fsutil"
)

// Local is a file store that stores files in the local file system.
type Local struct {
	basePath string
}

func NewLocal(basePath string) (*Local, error) {
	if !filepath.IsAbs(basePath) {
		return nil, errors.New("base path must be absolute")
	}
	err := fsutil.DirWritable(basePath)
	if err != nil {
		return nil, err
	}
	return &Local{
		basePath: basePath,
	}, nil
}

func (l *Local) Delete(ctx context.Context, relPath string) error {
	err := os.Remove(filepath.Join(l.basePath, filepath.FromSlash(relPath)))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (l *Local) Get(ctx context.Context, relPath string) (*File, io.ReadCloser, error) {
	absPath := filepath.Join(l.basePath, filepath.FromSlash(relPath))

	f, err := os.Open(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if fi.IsDir() {
		f.Close()
		return nil, nil, ErrNotFound
	}

	return &File{
		Modified: fi.ModTime(),
		Path:     relPath,
		Size:     fi.Size(),
	}, f, nil
}

func (l *Local) Head(ctx context.Context, relPath string) (*File, error) {
	absPath := filepath.Join(l.basePath, filepath.FromSlash(relPath))
	fi, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if fi.IsDir() {
		return nil, ErrNotFound
	}

	return &File{
		Modified: fi.ModTime(),
		Path:     relPath,
		Size:     fi.Size(),
	}, nil
}

func (l *Local) List(ctx context.Context, relPath string, recursive bool) (<-chan *File, <-chan error) {
	c := make(chan *File, 1)
	e := make(chan error, 1)

	go func() {
		defer close(e)
		defer close(c)

		absPath := filepath.Join(l.basePath, filepath.FromSlash(relPath))
		e <- filepath.WalkDir(absPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// A resource that is not found does not get listed.
					return nil
				}
				return err
			}

			if d.IsDir() {
				if !recursive && path != absPath {
					return fs.SkipDir
				}
				return nil
			}

			// Only return results for regular files.
			if !d.Type().IsRegular() {
				return nil
			}

			fi, err := d.Info()
			if err != nil {
				return err
			}

			relFilePath, err := filepath.Rel(l.basePath, path)
			if err != nil {
				return err
			}

			f := &File{
				Modified: fi.ModTime(),
				Path:     filepath.ToSlash(relFilePath),
				Size:     fi.Size(),
			}

			select {
			case c <- f:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()

	return c, e
}

func (l *Local) Put(ctx context.Context, relPath string, r io.Reader) (*File, error) {
	absPath := filepath.Join(l.basePath, filepath.FromSlash(relPath))

	dir, _ := filepath.Split(relPath)
	if dir != "" {
		err := os.MkdirAll(filepath.Dir(absPath), 0755)
		if err != nil {
			return nil, err
		}
	}

	f, err := os.Create(absPath)
	if err != nil {
		return nil, err
	}

	if r != nil {
		if _, err = io.Copy(f, r); err != nil {
			f.Close()
			os.Remove(absPath)
			return nil, err
		}
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		os.Remove(absPath)
		return nil, err
	}

	if err = f.Close(); err != nil {
		os.Remove(absPath)
		return nil, err
	}

	return &File{
		Modified: fi.ModTime(),
		Path:     relPath,
		Size:     fi.Size(),
	}, nil
}

func (l *Local) Type() string {
	return "local"
}
