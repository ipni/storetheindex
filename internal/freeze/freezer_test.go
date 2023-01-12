package freeze_test

import (
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/internal/disk"
	"github.com/ipni/storetheindex/internal/freeze"
	"github.com/stretchr/testify/require"
)

func TestCheckFreeze(t *testing.T) {
	tempDir := t.TempDir()

	du, err := disk.Usage(tempDir)
	require.NoError(t, err)

	dstore := datastore.NewMapDatastore()

	var freezeCount int
	freezeFunc := func() error {
		freezeCount++
		return nil
	}

	f, err := freeze.New(tempDir, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.False(t, f.Frozen())
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

	f, err = freeze.New(tempDir, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.False(t, f.Frozen())
	f.Close()

	require.Zero(t, freezeCount)

	f, err = freeze.New(tempDir, du.Percent/2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.True(t, f.Frozen())
	require.True(t, f.CheckNow())
	f.Close()

	require.Equal(t, 1, freezeCount)

	f, err = freeze.New(tempDir, du.Percent/2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.True(t, f.Frozen())
	f.Close()

	require.Equal(t, 1, freezeCount)
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

	f, err := freeze.New(tempDir, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.NoError(t, f.Freeze())
	require.Equal(t, 1, freezeCount)
	require.NoError(t, f.Freeze())
	require.NoError(t, f.Freeze())
	require.Equal(t, 1, freezeCount)
	f.Close()

	f, err = freeze.New(tempDir, du.Percent*2.0, dstore, freezeFunc)
	require.NoError(t, err)
	require.NoError(t, f.Freeze())
	f.Close()

	require.Equal(t, 1, freezeCount)
}
