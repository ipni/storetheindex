package freeze_test

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/fsutil/disk"
	"github.com/ipni/storetheindex/internal/freeze"
	"github.com/stretchr/testify/require"
)

func TestCheckFreeze(t *testing.T) {
	tempDir := t.TempDir()
	tempDir2 := t.TempDir()

	du, err := disk.Usage(tempDir)
	require.NoError(t, err)

	dstore := datastore.NewMapDatastore()

	var freezeCount int
	freezeFunc := func() error {
		freezeCount++
		return nil
	}
	dirs := []string{tempDir, tempDir2}
	f, err := freeze.New(dirs, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.False(t, f.Frozen())
	require.Equal(t, 1, len(f.Dirs()))

	du2, err := f.Usage()
	require.NoError(t, err)
	require.Equal(t, du.Path, du2.Path)

	checkChan := make(chan bool, 5)
	for i := 0; i < cap(checkChan); i++ {
		go func() {
			checkChan <- f.CheckNow()
		}()
	}
	for i := 0; i < cap(checkChan); i++ {
		checkResult := <-checkChan
		require.False(t, checkResult)
	}
	f.Close()

	// Check that freeze does not happen when enough storage.
	f, err = freeze.New(dirs, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.False(t, f.Frozen())
	f.Close()
	require.Zero(t, freezeCount)

	// Check that freeze happens when insufficient storage.
	f, err = freeze.New(dirs, du.Percent/2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.True(t, f.Frozen())
	require.True(t, f.CheckNow())
	f.Close()
	require.Equal(t, 1, freezeCount)

	// Check that freeze does not happen again.
	f, err = freeze.New(dirs, du.Percent/2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.True(t, f.Frozen())
	f.Close()
	require.Equal(t, 1, freezeCount)

	// Unfreeze no directories.
	err = freeze.Unfreeze(context.Background(), nil, du.Percent/2.0, dstore)
	require.NoError(t, err)

	// Unfreeze with insufficient storage.
	err = freeze.Unfreeze(context.Background(), dirs, du.Percent/2.0, dstore)
	require.ErrorContains(t, err, "cannot unfreeze")

	// Unfreeze with bad directory.
	badDir := filepath.Join(tempDir, "baddir")
	err = freeze.Unfreeze(context.Background(), []string{badDir}, du.Percent*2.0, dstore)
	require.ErrorIs(t, err, fs.ErrNotExist)

	// Unfreze should work here.
	err = freeze.Unfreeze(context.Background(), dirs, du.Percent*2.0, dstore)
	require.NoError(t, err)

	// Unfreeze already unfrozen.
	err = freeze.Unfreeze(context.Background(), dirs, du.Percent*2.0, dstore)
	require.NoError(t, err)

	// Freeze and check that unfreeze works when freeze is disabled.
	f, err = freeze.New(dirs, du.Percent/2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.True(t, f.Frozen())
	f.Close()
	require.Equal(t, 2, freezeCount)

	err = freeze.Unfreeze(context.Background(), dirs, -1, dstore)
	require.NoError(t, err)
}

func TestManualFreeze(t *testing.T) {
	tempDir := t.TempDir()

	du, err := disk.Usage(tempDir)
	require.NoError(t, err)

	dstore := datastore.NewMapDatastore()

	var freezeCount int
	freezeFunc := func() error {
		freezeCount++
		return nil
	}

	dirs := []string{tempDir}
	f, err := freeze.New(dirs, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.NoError(t, f.Freeze())
	require.Equal(t, 1, freezeCount)
	require.NoError(t, f.Freeze())
	require.NoError(t, f.Freeze())
	require.Equal(t, 1, freezeCount)
	f.Close()

	f, err = freeze.New(dirs, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.NoError(t, f.Freeze())
	f.Close()

	f, err = freeze.New(dirs, -1, dstore, freezeFunc)
	require.NoError(t, err)
	require.ErrorIs(t, f.Freeze(), freeze.ErrNoFreeze)
	require.True(t, f.Frozen())
	f.Close()

	require.Equal(t, 1, freezeCount)
}
