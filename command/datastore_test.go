package command

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
)

func TestCreateDatastore(t *testing.T) {
	tmpDir := t.TempDir()
	dsDir := filepath.Join(tmpDir, "testDataDir")
	_, _, err := createDatastore(context.Background(), dsDir, "unknown", false)
	require.Error(t, err)

	ds, path, err := createDatastore(context.Background(), dsDir, "levelds", false)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.Equal(t, dsDir, path)
	require.NoError(t, ds.Close())

	checkFile := filepath.Join(dsDir, "check.test")
	err = os.WriteFile(checkFile, []byte("Hello"), 0666)
	require.NoError(t, err)

	// Check that ds directory is not removed.
	ds, path, err = createDatastore(context.Background(), dsDir, "levelds", false)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.NoError(t, ds.Close())
	require.True(t, fileExists(checkFile))

	// Check that ds directory is removed.
	ds, path, err = createDatastore(context.Background(), dsDir, "levelds", true)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.NoError(t, ds.Close())
	require.False(t, fileExists(checkFile))
}

func TestDeletePrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	dsDir := filepath.Join(tmpDir, "testDataDir")
	ds, _, err := createDatastore(ctx, dsDir, "levelds", false)
	require.NoError(t, err)

	const prefix = "testKeys"
	dsKey1 := datastore.NewKey(prefix + "/foo")
	err = ds.Put(ctx, dsKey1, []byte("One"))
	require.NoError(t, err)
	dsKey2 := datastore.NewKey(prefix + "/bar")
	err = ds.Put(ctx, dsKey2, []byte("Two"))
	require.NoError(t, err)
	err = ds.Sync(ctx, datastore.NewKey(""))
	require.NoError(t, err)

	q := query.Query{
		Prefix: prefix,
	}
	results, err := ds.Query(ctx, q)
	require.NoError(t, err)
	ents, err := results.Rest()
	results.Close()
	require.NoError(t, err)
	require.Len(t, ents, 2)

	n, err := deletePrefix(ctx, ds, prefix)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	results, err = ds.Query(ctx, q)
	require.NoError(t, err)
	ents, err = results.Rest()
	results.Close()
	require.NoError(t, err)
	require.Empty(t, ents)
}

func fileExists(filename string) bool {
	_, err := os.Lstat(filename)
	return !errors.Is(err, os.ErrNotExist)
}
