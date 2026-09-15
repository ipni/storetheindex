package fsutil

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"time"
)

// DirWritable checks if a directory is writable. If the directory does
// not exist it is created with writable permission.
func DirWritable(dir string) error {
	if dir == "" {
		return errors.New("directory not specified")
	}

	var err error
	dir, err = ExpandHome(dir)
	if err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Directory does not exist, so create it.
			err = os.Mkdir(dir, 0775)
			if err == nil {
				return nil
			}
		}
		if errors.Is(err, fs.ErrPermission) {
			err = fs.ErrPermission
		}
		return fmt.Errorf("directory not writable: %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("not a directory: %s", dir)
	}

	// Directory exists, check that a file can be written.
	file, err := os.CreateTemp(dir, "writetest")
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			err = fs.ErrPermission
		}
		return fmt.Errorf("directory not writable: %s: %w", dir, err)
	}
	file.Close()
	return os.Remove(file.Name())
}

// ExpandHome expands the path to include the home directory if the path is
// prefixed with `~`. If it isn't prefixed with `~`, the path is returned
// as-is.
func ExpandHome(path string) (string, error) {
	if path == "" {
		return path, nil
	}

	if path[0] != '~' {
		return path, nil
	}

	if len(path) > 1 && path[1] != '/' && path[1] != '\\' {
		return "", errors.New("cannot expand user-specific home dir")
	}

	dir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, path[1:]), nil
}

// FileChanged returns the modification time of a file and true if different
// from the given time.
func FileChanged(filePath string, modTime time.Time) (time.Time, bool, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return modTime, false, fmt.Errorf("cannot stat file %s: %w", filePath, err)
	}
	if fi.ModTime() != modTime {
		return fi.ModTime(), true, nil
	}
	return modTime, false, nil
}

// FileExists return true if the file exists
func FileExists(filename string) bool {
	_, err := os.Lstat(filename)
	return !errors.Is(err, os.ErrNotExist)
}

// DirIter returns iterator over directory entries
//
// This method of directory iteration can be especially useful for iteration
// over directories with large number of entries. It does not read all entries
// upfront working on at most `batchSize` entries at a time
func DirIter(path string, batchSize int) iter.Seq2[os.DirEntry, error] {
	return func(yield func(os.DirEntry, error) bool) {
		dir, err := os.Open(path)
		if err != nil {
			yield(nil, err)
			return
		}

		defer dir.Close()

		for {
			entries, err := dir.ReadDir(batchSize)
			if errors.Is(err, io.EOF) {
				return
			} else if err != nil {
				yield(nil, err)
				return
			}

			for _, entry := range entries {
				if !yield(entry, nil) {
					return
				}
			}
		}
	}
}
