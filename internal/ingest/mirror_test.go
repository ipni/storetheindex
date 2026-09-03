package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	"github.com/ipni/storetheindex/test/goroutines"
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
			External: []config.StoreConfig{external},
		}, ds)
		require.NoError(t, err)
		require.False(t, m.canWrite())
		require.True(t, m.canRead())
		require.Nil(t, m.mainCarReader)
		require.Len(t, m.externalCarReaders, 1)
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
		require.Empty(t, m.externalCarReaders)
		require.NotNil(t, m.mainCarWriter)
		require.Nil(t, m.exposableFilestore)
	})

	t.Run("write only with external enables external read", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeWrite,
			Main:     main,
			External: []config.StoreConfig{external},
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.Nil(t, m.mainCarReader)
		require.Len(t, m.externalCarReaders, 1)
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
		require.Empty(t, m.externalCarReaders)
		require.Nil(t, m.mainCarWriter)
		require.NotNil(t, m.exposableFilestore)
	})

	t.Run("readwrite with external", func(t *testing.T) {
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     main,
			External: []config.StoreConfig{external},
		}, ds)
		require.NoError(t, err)
		require.True(t, m.canWrite())
		require.True(t, m.canRead())
		require.NotNil(t, m.mainCarReader)
		require.Len(t, m.externalCarReaders, 1)
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
		require.Empty(t, m.externalCarReaders)
	})

	t.Run("external same as main is an error when main used", func(t *testing.T) {
		_, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     main,
			External: []config.StoreConfig{main},
		}, ds)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be the same as the main backend")
	})

	t.Run("multiple externals", func(t *testing.T) {
		ext2 := localStoreConfig(t, "")
		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeWrite,
			Main:     main,
			External: []config.StoreConfig{external, ext2},
		}, ds)
		require.NoError(t, err)
		require.Len(t, m.externalCarReaders, 2)
	})

	t.Run("mixed compress applies independently to main and external", func(t *testing.T) {
		mainGzip := localStoreConfig(t, carstore.Gzip)
		externalNone := localStoreConfig(t, "none")

		m, err := newMirror(config.Mirror{
			MainMode: config.MainModeReadWrite,
			Main:     mainGzip,
			External: []config.StoreConfig{externalNone},
		}, ds)
		require.NoError(t, err)
		require.Equal(t, carstore.Gzip, m.mainCarWriter.Compression())
		require.Equal(t, carstore.Gzip, m.mainCarReader.Compression())
		require.Empty(t, m.externalCarReaders[0].Compression())
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
		External: []config.StoreConfig{external},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	adBlock, _, err := m.readMain(ctx, mainAdCid, true)
	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, mainAdCid, adBlock.Cid)

	adBlock, source, _, err := m.readExternalRace(ctx, externalAdCid, true)
	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, adDataSourceExternal, source)
	require.Equal(t, externalAdCid, adBlock.Cid)
}

func TestExternalRaceFirstWin(t *testing.T) {
	ctx := context.Background()
	empty := localStoreConfig(t, carstore.Gzip)
	withCAR := localStoreConfig(t, carstore.Gzip)
	adCid := writeAdCAR(t, withCAR, carstore.Gzip)

	m, err := newMirror(config.Mirror{
		External: []config.StoreConfig{empty, withCAR},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)
	require.Len(t, m.externalCarReaders, 2)

	adBlock, source, _, err := m.readExternalRace(ctx, adCid, true)
	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, adDataSourceExternal, source)
	require.Equal(t, adCid, adBlock.Cid)
}

func TestExternalRaceAllMiss(t *testing.T) {
	ctx := context.Background()
	empty1 := localStoreConfig(t, carstore.Gzip)
	empty2 := localStoreConfig(t, carstore.Gzip)
	adCid := writeAdCAR(t, localStoreConfig(t, carstore.Gzip), carstore.Gzip)

	m, err := newMirror(config.Mirror{
		External: []config.StoreConfig{empty1, empty2},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	_, source, _, err := m.readExternalRace(ctx, adCid, true)
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Equal(t, adDataSourceNone, source)
}

func TestExternalRaceCancelled(t *testing.T) {
	adCid, carPath, _ := makeAdCARBytes(t)

	// Slow peer: blocks until the request is cancelled so we exercise the
	// cancel path instead of an immediate local miss.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimPrefix(r.URL.Path, "/") != carPath {
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	m, err := newMirror(config.Mirror{
		External: []config.StoreConfig{httpStoreConfig(srv.URL)},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, _, err := m.readExternalRace(ctx, adCid, true)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// Depending on select timing, cancellation may surface as ctx.Err()
		// or as a peer miss (context.Canceled treated as miss → fs.ErrNotExist).
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, fs.ErrNotExist),
			"expected context.Canceled or fs.ErrNotExist, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("read did not return after cancel")
	}
}

func TestExternalRaceFastResponseBeatsDelayed(t *testing.T) {
	adCid, carPath, carData := makeAdCARBytes(t)

	delayed := startCARHTTPServer(t, map[string][]byte{carPath: carData}, carHTTPServeOpts{
		delayBeforeResponse: 500 * time.Millisecond,
	})
	fast := startCARHTTPServer(t, map[string][]byte{carPath: carData}, carHTTPServeOpts{})

	m, err := newMirror(config.Mirror{
		// Delayed mirror is listed first so a connection-order race would prefer it.
		External: []config.StoreConfig{httpStoreConfig(delayed), httpStoreConfig(fast)},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	start := time.Now()
	adBlock, source, _, err := m.readExternalRace(context.Background(), adCid, true)
	elapsed := time.Since(start)

	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, adDataSourceExternal, source)
	require.Equal(t, adCid, adBlock.Cid)
	require.Less(t, elapsed, 250*time.Millisecond,
		"fast mirror should win before delayed mirror responds; elapsed=%s", elapsed)
}

func TestExternalRaceSkipsWrongRootCAR(t *testing.T) {
	wantCid, wantPath, wantData := makeAdCARBytes(t)
	_, _, otherData := makeAdCARBytes(t)

	wrong := startCARHTTPServer(t, map[string][]byte{wantPath: otherData}, carHTTPServeOpts{})
	good := startCARHTTPServer(t, map[string][]byte{wantPath: wantData}, carHTTPServeOpts{})

	m, err := newMirror(config.Mirror{
		External: []config.StoreConfig{httpStoreConfig(wrong), httpStoreConfig(good)},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	adBlock, source, _, err := m.readExternalRace(context.Background(), wantCid, true)
	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, adDataSourceExternal, source)
	require.Equal(t, wantCid, adBlock.Cid)

	// Wrong-root-only mirrors are treated as misses.
	mWrongOnly, err := newMirror(config.Mirror{
		External: []config.StoreConfig{httpStoreConfig(wrong)},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)
	_, source, _, err = mWrongOnly.readExternalRace(context.Background(), wantCid, true)
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Equal(t, adDataSourceNone, source)
}

func TestExternalRaceFastBeatsThrottledBody(t *testing.T) {
	adCid, carPath, carData := makeAdCARBytes(t)
	require.Greater(t, len(carData), 20, "CAR should be large enough for throttling to matter")

	throttled := startCARHTTPServer(t, map[string][]byte{carPath: carData}, carHTTPServeOpts{
		byteInterval: 100 * time.Millisecond,
	})
	fast := startCARHTTPServer(t, map[string][]byte{carPath: carData}, carHTTPServeOpts{})

	m, err := newMirror(config.Mirror{
		External: []config.StoreConfig{httpStoreConfig(throttled), httpStoreConfig(fast)},
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	// Throttled mirror would need ~len(carData)*100ms to finish Read; fast should win quickly.
	slowestPlausibleWin := time.Duration(len(carData)/2) * 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	adBlock, source, _, err := m.readExternalRace(ctx, adCid, true)
	elapsed := time.Since(start)

	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, adDataSourceExternal, source)
	require.Equal(t, adCid, adBlock.Cid)
	require.Less(t, elapsed, slowestPlausibleWin,
		"fast mirror should win before throttled body delivers ad block; elapsed=%s carBytes=%d", elapsed, len(carData))
}

// TestExternalRaceLosersReleaseEntryReaders checks that the mirrors which lose
// the race do not leave entry readers behind. A loser that already read its ad
// block has an entry reader streaming the rest of its CAR, and nothing ever
// receives those entries, so cancelling the loser must release it along with
// the HTTP response body it holds open.
func TestExternalRaceLosersReleaseEntryReaders(t *testing.T) {
	const nPeers = 4

	adCid, carPath, carData := makeAdCARBytesWithEntries(t, 5)

	extCfgs := make([]config.StoreConfig, 0, nPeers)
	for range nPeers {
		srv := startCARHTTPServer(t, map[string][]byte{carPath: carData}, carHTTPServeOpts{})
		extCfgs = append(extCfgs, httpStoreConfig(srv))
	}

	m, err := newMirror(config.Mirror{
		External: extCfgs,
	}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)

	ctx := t.Context()

	adBlock, source, _, err := m.readExternalRace(ctx, adCid, false)
	require.NoError(t, err)
	defer adBlock.Close()
	require.Equal(t, adDataSourceExternal, source)
	require.NotNil(t, adBlock.Entries)

	var entryBlocks int
	for entBlock := range adBlock.Entries {
		require.NoError(t, entBlock.Err)
		entryBlocks++
	}
	require.NotZero(t, entryBlocks, "winning mirror must deliver entry blocks")

	goroutines.RequireNone(t, "carstore.readEntries")
}

type carHTTPServeOpts struct {
	delayBeforeResponse time.Duration
	byteInterval        time.Duration
}

func startCARHTTPServer(t *testing.T, files map[string][]byte, opts carHTTPServeOpts) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		relPath := strings.TrimPrefix(r.URL.Path, "/")
		data, ok := files[relPath]
		if !ok {
			http.NotFound(w, r)
			return
		}
		if opts.delayBeforeResponse > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(opts.delayBeforeResponse):
			}
		}
		w.WriteHeader(http.StatusOK)
		if opts.byteInterval <= 0 {
			_, _ = w.Write(data)
			return
		}
		flusher, _ := w.(http.Flusher)
		for i := range data {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(opts.byteInterval):
			}
			if _, err := w.Write(data[i : i+1]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func httpStoreConfig(baseURL string) config.StoreConfig {
	return config.StoreConfig{
		Compress: "none",
		Config: filestore.Config{
			Type: "http",
			HTTP: filestore.HTTPConfig{BaseURL: baseURL + "/"},
		},
	}
}

func makeAdCARBytes(t *testing.T) (cid.Cid, string, []byte) {
	t.Helper()
	return makeAdCARBytesWithEntries(t, 0)
}

// makeAdCARBytesWithEntries returns the CID, path, and uncompressed CAR bytes
// for an advertisement with entryChunks entry chunks. Only a CAR that has
// entries makes CarReader.Read stream entry blocks.
func makeAdCARBytesWithEntries(t *testing.T, entryChunks int) (cid.Cid, string, []byte) {
	t.Helper()
	storeCfg := localStoreConfig(t, "none")
	adCid := writeAdCARWithEntries(t, storeCfg, "none", entryChunks)
	fileStore, err := filestore.MakeFilestore(storeCfg.Config)
	require.NoError(t, err)
	carPath := adCid.String() + carstore.CarFileSuffix
	_, r, err := fileStore.Get(context.Background(), carPath)
	require.NoError(t, err)
	defer r.Close()
	carData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NotEmpty(t, carData)
	return adCid, carPath, carData
}

func writeAdCAR(t *testing.T, storeCfg config.StoreConfig, compress string) cid.Cid {
	t.Helper()
	return writeAdCARWithEntries(t, storeCfg, compress, 0)
}

func writeAdCARWithEntries(t *testing.T, storeCfg config.StoreConfig, compress string, entryChunks int) cid.Cid {
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

	var entries ipld.Link = schema.NoEntries
	if entryChunks != 0 {
		var next ipld.Link
		for range entryChunks {
			chunk := &schema.EntryChunk{
				Entries: random.Multihashes(4),
				Next:    next,
			}
			node, err := chunk.ToNode()
			require.NoError(t, err)
			next, err = lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
			require.NoError(t, err)
		}
		entries = next
	}

	p, priv, _ := random.Identity()
	adv := &schema.Advertisement{
		Provider:  p.String(),
		Addresses: []string{"/ip4/127.0.0.1/tcp/9999"},
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		Entries:   entries,
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
	_, err = carw.Write(context.Background(), adCid, entryChunks == 0, false)
	require.NoError(t, err)
	return adCid
}
