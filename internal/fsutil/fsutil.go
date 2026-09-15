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

// DirReadWriteCheck expands dir (including ~), ensures it exists as a
// directory (creating it if needed), and reports whether files can be created
// in it. The returned path is the expanded directory.
func DirReadWriteCheck(dir string) (string, bool, error) {
	if dir == "" {
		return "", false, errors.New("directory not specified")
	}

	dir, err := ExpandHome(dir)
	if err != nil {
		return "", false, err
	}

	switch fi, err := os.Stat(dir); {
	case errors.Is(err, fs.ErrNotExist):
		// Try to create the directory
		if err = os.Mkdir(dir, 0775); err != nil {
			return "", false, fmt.Errorf("cannot create directory: %s: %w", dir, simplifyDirErr(err))
		}

	case err != nil:
		return "", false, fmt.Errorf("directory not accessible: %s: %w", dir, simplifyDirErr(err))

	case !fi.IsDir():
		return "", false, fmt.Errorf("not a directory: %s", dir)
	}

	file, err := os.CreateTemp(dir, "writetest")
	if err != nil {
		return dir, false, nil
	}
	_ = file.Close()

	if err = os.Remove(file.Name()); err != nil {
		return dir, true, err
	}

	return dir, true, nil
}

func simplifyDirErr(err error) error {
	if errors.Is(err, fs.ErrPermission) {
		return fs.ErrPermission
	}
	return err
}

// DirWritable checks if a directory is writable. If the directory does
// not exist it is created with writable permission.
func DirWritable(dir string) error {
	dir, writable, err := DirReadWriteCheck(dir)
	if err != nil {
		return err
	}
	if !writable {
		return fmt.Errorf("directory not writable: %s: %w", dir, fs.ErrPermission)
	}
	return nil
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
