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

func TestNewMirrorMainModeOnlyAffectMain(t *testing.T) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	main := localFilestoreConfig(t)
	external := localFilestoreConfig(t)

	t.Run("external only enables read without MainMode", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			External: external,
		}, ds)
		require.NoError(t, err)
		require.False(t, m.canWrite())
		require.True(t, m.canRead())
		require.Nil(t, m.mainCarReader)
		require.NotNil(t, m.externalCarReader)
		require.Nil(t, m.mainCarWriter)
	})

	t.Run("write only does not enable main read", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeWrite,
			Main:     main,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.False(t, m.canRead())
		require.Nil(t, m.mainCarReader)
		require.Nil(t, m.externalCarReader)
		require.NotNil(t, m.mainCarWriter)
		require.Nil(t, m.exposableFilestore)
	})

	t.Run("write only with external enables external read", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeWrite,
			Main:     main,
			External: external,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.Nil(t, m.mainCarReader)
		require.NotNil(t, m.externalCarReader)
		require.NotNil(t, m.mainCarWriter)
		require.Nil(t, m.exposableFilestore)
	})

	t.Run("read only enables main read not write", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeRead,
			Main:     main,
		}, ds)
		require.NoError(t, err)
		require.False(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.mainCarReader)
		require.Nil(t, m.externalCarReader)
		require.Nil(t, m.mainCarWriter)
		require.NotNil(t, m.exposableFilestore)
	})

	t.Run("readwrite with external", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     main,
			External: external,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.mainCarReader)
		require.NotNil(t, m.externalCarReader)
		require.NotNil(t, m.mainCarWriter)
		require.NotNil(t, m.exposableFilestore)
	})

	t.Run("readwrite without external", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     main,
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.mainCarReader)
		require.Nil(t, m.externalCarReader)
	})

	t.Run("external same as main is an error when main used", func(t *testing.T) {
		_, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     main,
			External: main,
		}, ds)
		require.Error(t, err)
		require.Contains(t, err.Error(), "external retrieval cannot be the same as the main backend")
	})
}
