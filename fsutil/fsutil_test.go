package fsutil_test

import (
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
