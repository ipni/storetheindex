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
		require.Nil(t, m.carReader)
		require.NotNil(t, m.carExternalReader)
		require.Nil(t, m.carWriter)
		require.False(t, m.rdWrSame)
	})

	t.Run("write only does not enable local read", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeWrite,
			Local:     local,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.False(t, m.canRead())
		require.Nil(t, m.carReader)
		require.Nil(t, m.carExternalReader)
		require.NotNil(t, m.carWriter)
		require.Nil(t, m.exposableFilestore)
		require.False(t, m.rdWrSame)
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
		require.Nil(t, m.carReader)
		require.NotNil(t, m.carExternalReader)
		require.NotNil(t, m.carWriter)
		require.Nil(t, m.exposableFilestore)
		require.False(t, m.rdWrSame)
	})

	t.Run("read only enables local read not write", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeRead,
			Local:     local,
		}, ds)
		require.NoError(t, err)
		require.False(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.carReader)
		require.Nil(t, m.carExternalReader)
		require.Nil(t, m.carWriter)
		require.NotNil(t, m.exposableFilestore)
		require.False(t, m.rdWrSame)
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
		require.NotNil(t, m.carReader)
		require.NotNil(t, m.carExternalReader)
		require.NotNil(t, m.carWriter)
		require.NotNil(t, m.exposableFilestore)
		require.True(t, m.rdWrSame)
	})

	t.Run("readwrite without external", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeReadWrite,
			Local:     local,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.carReader)
		require.Nil(t, m.carExternalReader)
		require.True(t, m.rdWrSame)
	})

	t.Run("external same as local is disabled when local used", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			LocalMode: config.LocalModeReadWrite,
			Local:     local,
			External:  local,
		}, ds)
		require.NoError(t, err)
		require.NotNil(t, m.carReader)
		require.Nil(t, m.carExternalReader)
		require.True(t, m.rdWrSame)
	})
}
