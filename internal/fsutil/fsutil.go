package fsutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
)

// DirWritable checks the the directory is writable.  If the directory does
// not exist it is created with writable permission.
func DirWritable(dir string) error {
	if dir == "" {
		return errors.New("cannot check empty directory")
	}

	var err error
	dir, err = homedir.Expand(dir)
	if err != nil {
		return err
	}

	_, err = os.Stat(dir)
	if err == nil {
		// dir exists, make sure we can write to it
		testfile := filepath.Join(dir, "test")
		fi, err := os.Create(testfile)
		if err != nil {
			if os.IsPermission(err) {
				return fmt.Errorf("%s is not writeable by the current user", dir)
			}
			return fmt.Errorf("unexpected error while checking directory writeablility: %w", err)
		}
		fi.Close()
		return os.Remove(testfile)
	}

	if os.IsNotExist(err) {
		// dir doesn't exist, check that we can create it
		return os.Mkdir(dir, 0775)
	}

	if os.IsPermission(err) {
		return fmt.Errorf("cannot write to %s, incorrect permissions", dir)
	}

	return err
}

// FileExists return true if the file exists
func FileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	if fi != nil || (err != nil && !os.IsNotExist(err)) {
		return true
	}
	return false
}
