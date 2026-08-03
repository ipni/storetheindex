package ingest

import (
	"testing"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"
)

func localFilestoreConfig(t *testing.T) filestore.Config {
	t.Helper()
	return filestore.Config{
		Type: "local",
		Local: filestore.LocalConfig{
			BasePath: t.TempDir(),
		},
	}
}

func TestNewMirrorLocalModeOnlyAffectLocal(t *testing.T) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	local := localFilestoreConfig(t)
	external := localFilestoreConfig(t)

	t.Run("external only enables read without LocalMode", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			External: external,
		}, ds)
		require.NoError(t, err)
		require.False(t, m.canWrite())
		require.True(t, m.canRead())
		require.Nil(t, m.localCarReader)
		require.NotNil(t, m.externalCarReader)
		require.Nil(t, m.localCarWriter)
	})

	t.Run("write only does not enable local read", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeWrite,
			Local:     local,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.False(t, m.canRead())
		require.Nil(t, m.localCarReader)
		require.Nil(t, m.externalCarReader)
		require.NotNil(t, m.localCarWriter)
		require.Nil(t, m.exposableFilestore)
	})

	t.Run("write only with external enables external read", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeWrite,
			Local:     local,
			External:  external,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.Nil(t, m.localCarReader)
		require.NotNil(t, m.externalCarReader)
		require.NotNil(t, m.localCarWriter)
		require.Nil(t, m.exposableFilestore)
	})

	t.Run("read only enables local read not write", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeRead,
			Local:     local,
		}, ds)
		require.NoError(t, err)
		require.False(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.localCarReader)
		require.Nil(t, m.externalCarReader)
		require.Nil(t, m.localCarWriter)
		require.NotNil(t, m.exposableFilestore)
	})

	t.Run("readwrite with external", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeReadWrite,
			Local:     local,
			External:  external,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.localCarReader)
		require.NotNil(t, m.externalCarReader)
		require.NotNil(t, m.localCarWriter)
		require.NotNil(t, m.exposableFilestore)
	})

	t.Run("readwrite without external", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeReadWrite,
			Local:     local,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.localCarReader)
		require.Nil(t, m.externalCarReader)
	})

	t.Run("external same as local is an error when local used", func(t *testing.T) {
		_, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeReadWrite,
			Local:     local,
			External:  local,
		}, ds)
		require.Error(t, err)
		require.Contains(t, err.Error(), "external retrieval cannot be the same as the local backend")
	})
}
