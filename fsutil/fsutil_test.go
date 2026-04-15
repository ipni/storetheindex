package fsutil_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ipni/storetheindex/fsutil"
	"github.com/stretchr/testify/require"
)

func TestDirWritable(t *testing.T) {
	err := fsutil.DirWritable("")
	require.Error(t, err)

	err = fsutil.DirWritable("~nosuchuser/tmp")
	require.Error(t, err)

	tmpDir := t.TempDir()

	wrDir := filepath.Join(tmpDir, "readwrite")
	err = fsutil.DirWritable(wrDir)
	require.NoError(t, err)

	// Check that DirWritable created directory.
	fi, err := os.Stat(wrDir)
	require.NoError(t, err)
	require.True(t, fi.IsDir())

	err = fsutil.DirWritable(wrDir)
	require.NoError(t, err)

	// If running on Windows, skip read-only directory tests.
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}

	roDir := filepath.Join(tmpDir, "readonly")
	require.NoError(t, os.Mkdir(roDir, 0500))
	err = fsutil.DirWritable(roDir)
	require.ErrorIs(t, err, fs.ErrPermission)

	roChild := filepath.Join(roDir, "child")
	err = fsutil.DirWritable(roChild)
	require.ErrorIs(t, err, fs.ErrPermission)
}

func TestFileChanged(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	var modTime time.Time
	var changed bool
	modTime, changed, err = fsutil.FileChanged(file.Name(), modTime)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, modTime.IsZero())

	var newModTime time.Time
	newModTime, changed, err = fsutil.FileChanged(file.Name(), modTime)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, modTime, newModTime)

	_, _, err = fsutil.FileChanged(filepath.Join(t.TempDir(), "nosuchfile"), modTime)
	require.Error(t, err)
}

func TestFileExists(t *testing.T) {
	fileName := filepath.Join(t.TempDir(), "somefile")
	require.False(t, fsutil.FileExists(fileName))

	file, err := os.Create(fileName)
	require.NoError(t, err)
	file.Close()

	require.True(t, fsutil.FileExists(fileName))
}

func TestExpandHome(t *testing.T) {
	dir, err := fsutil.ExpandHome("")
	require.NoError(t, err)
	require.Equal(t, "", dir)

	origDir := filepath.Join("somedir", "somesub")
	dir, err = fsutil.ExpandHome(origDir)
	require.NoError(t, err)
	require.Equal(t, origDir, dir)

	_, err = fsutil.ExpandHome(filepath.FromSlash("~nosuchuser/somedir"))
	require.Error(t, err)

	homeEnv := "HOME"
	if runtime.GOOS == "windows" {
		homeEnv = "USERPROFILE"
	}
	origHome := os.Getenv(homeEnv)
	defer os.Setenv(homeEnv, origHome)
	homeDir := filepath.Join(t.TempDir(), "testhome")
	os.Setenv(homeEnv, homeDir)

	const subDir = "mytmp"
	origDir = filepath.Join("~", subDir)
	dir, err = fsutil.ExpandHome(origDir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(homeDir, subDir), dir)

	os.Unsetenv(homeEnv)
	_, err = fsutil.ExpandHome(origDir)
	require.Error(t, err)
}

func TestDirIter(t *testing.T) {
	const testEntries = 1000
	const fileNamesPrimeFactor = 7919

	dir := t.TempDir()

	fileNames := make([]string, 0, testEntries)

	for i := range testEntries {
		fileName := fmt.Sprintf("test-file-%d", (i*fileNamesPrimeFactor)%testEntries)
		fileNames = append(fileNames, fileName)
		require.NoError(t, os.WriteFile(filepath.Join(dir, fileName), fmt.Appendf(nil, "content-%d", i), 0600))
	}

	for _, batchSize := range []int{1, 2, 5, 100, testEntries, 2 * testEntries} {
		t.Run(fmt.Sprintf("read - batch size:%d", batchSize), func(t *testing.T) {
			readBackEntries := make([]string, 0, testEntries)

			for entry, err := range fsutil.DirIter(dir, batchSize) {
				require.NoError(t, err)
				readBackEntries = append(readBackEntries, entry.Name())
			}

			require.ElementsMatch(t, fileNames, readBackEntries)
		})
	}

	t.Run("directory not found", func(t *testing.T) {
		loopVisited := false
		for entry, err := range fsutil.DirIter(filepath.Join(dir, "im-not-here"), 100) {
			require.Nil(t, entry, "expecting error for non-existing directory")
			require.ErrorIs(t, err, fs.ErrNotExist, "must return proper error")
			require.False(t, loopVisited, "more than one iteration detected")
			loopVisited = true
		}
		require.True(t, loopVisited, "there must be exactly one loop body iteration")
	})

	t.Run("properly handle early exit", func(t *testing.T) {
		const entriesLimit = 100

		entriesProcessed := 0

		for _, err := range fsutil.DirIter(dir, 100) {
			require.NoError(t, err)
			require.Less(t, entriesProcessed, entriesLimit)
			entriesProcessed++

			if entriesProcessed == entriesLimit {
				break
			}
		}

		require.Equal(t, entriesLimit, entriesProcessed)
	})

	t.Run("no permission to read", func(t *testing.T) {
		dir2 := t.TempDir()
		require.NoError(t, os.Chmod(dir2, 0))
		for _, err := range fsutil.DirIter(dir2, 100) {
			require.Error(t, err)
		}
	})
}
