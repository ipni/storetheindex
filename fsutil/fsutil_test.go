package fsutil_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipni/storetheindex/fsutil"
	"github.com/stretchr/testify/require"
)

// These tests only smoke-test the public shim. Behavior is covered in
// internal/fsutil.

func TestShimDirWritable(t *testing.T) {
	require.NoError(t, fsutil.DirWritable(t.TempDir()))
}

func TestShimExpandHome(t *testing.T) {
	dir, err := fsutil.ExpandHome("somedir")
	require.NoError(t, err)
	require.Equal(t, "somedir", dir)
}

func TestShimFileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	require.False(t, fsutil.FileExists(path))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0600))
	require.True(t, fsutil.FileExists(path))
}

func TestShimFileChanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0600))
	_, changed, err := fsutil.FileChanged(path, time.Time{})
	require.NoError(t, err)
	require.True(t, changed)
}

func TestShimDirIter(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("x"), 0600))
	var names []string
	for e, err := range fsutil.DirIter(dir, 10) {
		require.NoError(t, err)
		names = append(names, e.Name())
	}
	require.Equal(t, []string{"a"}, names)
}
