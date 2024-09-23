package fsutil

import (
	"errors"
	"fmt"
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
	dir, err = Expand(dir)
	if err != nil {
		return err
	}

	if _, err = os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// dir doesn't exist, check that we can create it
			err = os.Mkdir(dir, 0o775)
			if err == nil {
				return nil
			}
		}
		if errors.Is(err, os.ErrPermission) {
			err = os.ErrPermission
		}
		return fmt.Errorf("cannot write to %s: %w", dir, err)
	}

	// dir exists, make sure we can write to it
	file, err := os.CreateTemp(dir, "test")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			err = os.ErrPermission
		}
		return fmt.Errorf("cannot write to %s: %w", dir, err)
	}
	file.Close()
	return os.Remove(file.Name())
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

// Expand expands the path to include the home directory if the path is
// prefixed with `~`. If it isn't prefixed with `~`, the path is returned
// as-is.
func Expand(path string) (string, error) {
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
