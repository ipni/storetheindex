package ingest

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"
)

func localStoreConfig(t *testing.T, compress string) config.StoreConfig {
	t.Helper()
	return config.StoreConfig{
		Compress: compress,
		Config: filestore.Config{
			Type: "local",
			Local: filestore.LocalConfig{
				BasePath: t.TempDir(),
			},
		},
	}
}

func TestNewMirrorMainModeOnlyAffectMain(t *testing.T) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	main := localStoreConfig(t, "")
	external := localStoreConfig(t, "")

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

	t.Run("mixed compress applies independently to main and external", func(t *testing.T) {
		mainGzip := localStoreConfig(t, carstore.Gzip)
		externalNone := localStoreConfig(t, "none")

		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     mainGzip,
			External: externalNone,
		}, ds)
		require.NoError(t, err)
		require.Equal(t, carstore.Gzip, m.mainCarWriter.Compression())
		require.Equal(t, carstore.Gzip, m.mainCarReader.Compression())
		require.Empty(t, m.externalCarReader.Compression())
	})
}

func TestNewMirrorMixedCompressionRead(t *testing.T) {
	ctx := context.Background()
	main := localStoreConfig(t, carstore.Gzip)
	external := localStoreConfig(t, "none")

	mainAdCid := writeAdCAR(t, main, carstore.Gzip)
	externalAdCid := writeAdCAR(t, external, "none")

	m, err := newMirror(config.Mirror{
		MainMode: config.MainModeReadWrite,
		Main:     main,
		External: external,
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	adBlock, source, err := m.read(ctx, mainAdCid, true)
	require.NoError(t, err)
	require.Equal(t, adDataSourceMain, source)
	require.Equal(t, mainAdCid, adBlock.Cid)

	adBlock, source, err = m.read(ctx, externalAdCid, true)
	require.NoError(t, err)
	require.Equal(t, adDataSourceExternal, source)
	require.Equal(t, externalAdCid, adBlock.Cid)
}

func writeAdCAR(t *testing.T, storeCfg config.StoreConfig, compress string) cid.Cid {
	t.Helper()

	dstore := datastore.NewMapDatastore()
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		val, err := dstore.Get(lctx.Ctx, datastore.NewKey(lnk.(cidlink.Link).Cid.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			return dstore.Put(lctx.Ctx, datastore.NewKey(lnk.(cidlink.Link).Cid.String()), buf.Bytes())
		}, nil
	}

	p, priv, _ := random.Identity()
	adv := &schema.Advertisement{
		Provider:  p.String(),
		Addresses: []string{"/ip4/127.0.0.1/tcp/9999"},
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		Entries:   schema.NoEntries,
	}
	require.NoError(t, adv.Sign(priv))
	node, err := adv.ToNode()
	require.NoError(t, err)
	adLink, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)

	fileStore, err := filestore.MakeFilestore(storeCfg.Config)
	require.NoError(t, err)
	carw, err := carstore.NewWriter(dstore, fileStore, carstore.WithCompress(compress))
	require.NoError(t, err)

	adCid := adLink.(cidlink.Link).Cid
	_, err = carw.Write(context.Background(), adCid, true, false)
	require.NoError(t, err)
	return adCid
}
