package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestNewFillerRequiresReadWrite(t *testing.T) {
	main := localStore(t)
	_, err := newFiller(Options{Mirror: config.Mirror{MainMode: config.MainModeRead, Main: main}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "readwrite")

	_, err = newFiller(Options{Mirror: config.Mirror{MainMode: config.MainModeWrite, Main: main}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "readwrite")
}

func TestFillRequiresStartAd(t *testing.T) {
	_, err := Fill(context.Background(), Options{Mirror: rwMirror(localStore(t))})
	require.Error(t, err)
	require.Contains(t, err.Error(), "LastAdvertisement")
}

func TestApplyIndexerUsesLastAdvertisement(t *testing.T) {
	provider, _, _ := random.Identity()
	publisher, _, _ := random.Identity()
	lastAd := random.Cids(1)[0]

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/providers/"+provider.String(), r.URL.Path)
		require.NoError(t, json.NewEncoder(w).Encode(model.ProviderInfo{
			AddrInfo:          peer.AddrInfo{ID: provider},
			LastAdvertisement: lastAd,
			Publisher:         &peer.AddrInfo{ID: publisher},
		}))
	}))
	t.Cleanup(srv.Close)

	var opts Options
	require.NoError(t, applyIndexer(context.Background(), srv.URL, provider, &opts))
	require.Equal(t, lastAd, opts.StartAd)
	require.Equal(t, publisher, opts.Publisher.ID)
}

func TestLocalConfigResolvesMirrorDir(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(cfgFile, []byte(`{
  "Version": 2,
  "Ingest": {
    "AdvertisementMirror": {
      "MainMode": "readwrite",
      "Main": {
        "Type": "local",
        "Compress": "gzip",
        "Local": {
          "BasePath": "mirror",
          "DefaultPathSplit": [11, 2]
        }
      },
      "External": [
        {
          "Type": "http",
          "Compress": "gzip",
          "HTTP": {
            "BaseURL": "https://sf.cid.contact/carmirror/"
          }
        }
      ]
    }
  }
}`), 0o644))

	cfg, err := config.Load(cfgFile)
	require.NoError(t, err)
	require.Equal(t, config.MainModeReadWrite, cfg.Ingest.AdvertisementMirror.MainMode)
	require.Equal(t, "https://sf.cid.contact/carmirror/", cfg.Ingest.AdvertisementMirror.External[0].HTTP.BaseURL)

	require.NoError(t, resolveRelativeMirrorPaths(cfg, cfgFile))
	require.True(t, filepath.IsAbs(cfg.Ingest.AdvertisementMirror.Main.Local.BasePath))
	require.Equal(t, "mirror", filepath.Base(cfg.Ingest.AdvertisementMirror.Main.Local.BasePath))
	st, err := os.Stat(cfg.Ingest.AdvertisementMirror.Main.Local.BasePath)
	require.NoError(t, err)
	require.True(t, st.IsDir())
}

func TestFillAlreadyOnMain(t *testing.T) {
	ctx := context.Background()
	main := localStore(t)
	ds := datastore.NewMapDatastore()
	ad2, ad1 := storeAdChain(t, ds, 2)

	writeCAR(t, ds, main, ad2.cid)
	writeCAR(t, cloneDS(t, ds), main, ad1.cid)

	st, err := Fill(ctx, Options{
		Mirror:   rwMirror(main),
		Provider: ad2.provider,
		StartAd:  ad2.cid,
	})
	require.NoError(t, err)
	require.Equal(t, 2, st.Scanned)
	require.Equal(t, 2, st.AlreadyPresent)
	require.Zero(t, st.Downloaded)
	require.Zero(t, st.CopiedExternal)
	require.Equal(t, stopGenesis, st.StopReason)
}

func TestFillCopiesFromExternal(t *testing.T) {
	ctx := context.Background()
	main := localStore(t)
	ext := localStore(t)
	ds := datastore.NewMapDatastore()
	ad2, ad1 := storeAdChain(t, ds, 2)

	writeCAR(t, cloneDS(t, ds), ext, ad2.cid)
	writeCAR(t, cloneDS(t, ds), ext, ad1.cid)

	st, err := Fill(ctx, Options{
		Mirror:  rwMirror(main, ext),
		StartAd: ad2.cid,
	})
	require.NoError(t, err)
	require.Equal(t, 2, st.CopiedExternal)
	require.Zero(t, st.AlreadyPresent)
	require.Zero(t, st.Downloaded)
	require.Equal(t, stopGenesis, st.StopReason)

	reader, err := carstore.NewReader(mustStore(t, main), carstore.WithCompress(main.Compress))
	require.NoError(t, err)
	for _, c := range []cid.Cid{ad2.cid, ad1.cid} {
		block, err := reader.Read(ctx, c, false)
		require.NoError(t, err)
		_, err = inspectCar(c, block)
		require.NoError(t, err)
	}
}

func TestFillStopsOnInvalidMainCAR(t *testing.T) {
	ctx := context.Background()
	main := localStore(t)
	ds := datastore.NewMapDatastore()
	ad2, ad1 := storeAdChain(t, ds, 2)
	writeCAR(t, cloneDS(t, ds), main, ad1.cid)

	fs, err := filestore.MakeFilestore(main.Config)
	require.NoError(t, err)
	_, err = fs.Put(ctx, ad2.cid.String()+carstore.CarFileSuffix+carstore.GzipFileSuffix, bytes.NewReader([]byte("not-a-car")))
	require.NoError(t, err)

	st, err := Fill(ctx, Options{
		Mirror:  rwMirror(main),
		StartAd: ad2.cid,
	})
	require.Error(t, err)
	require.Equal(t, stopInvalidCar, st.StopReason)
	require.Equal(t, 1, st.Scanned)
}

func TestFillDepthLimit(t *testing.T) {
	ctx := context.Background()
	main := localStore(t)
	ds := datastore.NewMapDatastore()
	ad3, ad2, ad1 := storeAdChain3(t, ds)
	for _, ad := range []testAd{ad3, ad2, ad1} {
		writeCAR(t, cloneDS(t, ds), main, ad.cid)
	}

	st, err := Fill(ctx, Options{
		Mirror:  rwMirror(main),
		StartAd: ad3.cid,
		Depth:   2,
	})
	require.NoError(t, err)
	require.Equal(t, 2, st.Scanned)
	require.Equal(t, 2, st.AlreadyPresent)
	require.Equal(t, stopDepth, st.StopReason)
}

func TestFillWritesAdOnlyForNoEntries(t *testing.T) {
	ctx := context.Background()
	main := localStore(t)
	ext := localStore(t)
	ds := datastore.NewMapDatastore()

	empty := storeAd(t, ds, 0, nil)
	withEnts := storeAd(t, ds, 2, cidlink.Link{Cid: empty.cid})
	writeCAR(t, cloneDS(t, ds), ext, withEnts.cid)
	writeCAR(t, cloneDS(t, ds), ext, empty.cid)

	st, err := Fill(ctx, Options{
		Mirror:  rwMirror(main, ext),
		StartAd: withEnts.cid,
	})
	require.NoError(t, err)
	require.Equal(t, 2, st.CopiedExternal)
	require.Equal(t, 1, st.SkippedNoEnts)
	require.Equal(t, 2, st.Scanned)
	require.Equal(t, stopGenesis, st.StopReason)

	reader, err := carstore.NewReader(mustStore(t, main), carstore.WithCompress(main.Compress))
	require.NoError(t, err)
	block, err := reader.Read(ctx, empty.cid, true)
	require.NoError(t, err)
	require.NoError(t, block.Close())
}

func TestFillDownloadsFromProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pubDS := dssync.MutexWrap(datastore.NewMapDatastore())
	ad2, ad1 := storeAdChain(t, pubDS, 2)

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	lsys := mkLinkSystem(pubDS)
	pub, err := ipnisync.NewPublisher(lsys, priv, ipnisync.WithHTTPListenAddrs("127.0.0.1:0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	pub.SetRoot(ad2.cid)

	main := localStore(t)
	st, err := Fill(ctx, Options{
		Mirror:      rwMirror(main),
		StartAd:     ad2.cid,
		Publisher:   peer.AddrInfo{ID: pub.ID(), Addrs: pub.Addrs()},
		HttpTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 2, st.Downloaded)
	require.Zero(t, st.AlreadyPresent)
	require.Equal(t, stopGenesis, st.StopReason)
	require.Positive(t, st.BytesWritten)
	require.Positive(t, st.BytesDownloaded)

	reader, err := carstore.NewReader(mustStore(t, main), carstore.WithCompress(main.Compress))
	require.NoError(t, err)
	for _, c := range []cid.Cid{ad2.cid, ad1.cid} {
		block, err := reader.Read(ctx, c, false)
		require.NoError(t, err)
		data, err := inspectCar(c, block)
		require.NoError(t, err)
		require.False(t, data.hamt)
		require.NotZero(t, data.chunks)
	}
}

func TestFillUsesEachSourceOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pubDS := dssync.MutexWrap(datastore.NewMapDatastore())
	ad3, ad2, _ := storeAdChain3(t, pubDS)

	main := localStore(t)
	ext := localStore(t)
	writeCAR(t, cloneDS(t, pubDS), main, ad3.cid)
	writeCAR(t, cloneDS(t, pubDS), ext, ad2.cid)

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	pub, err := ipnisync.NewPublisher(mkLinkSystem(pubDS), priv, ipnisync.WithHTTPListenAddrs("127.0.0.1:0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pub.Close() })
	pub.SetRoot(ad3.cid)

	st, err := Fill(ctx, Options{
		Mirror:      rwMirror(main, ext),
		StartAd:     ad3.cid,
		Publisher:   peer.AddrInfo{ID: pub.ID(), Addrs: pub.Addrs()},
		HttpTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 1, st.AlreadyPresent)
	require.Equal(t, 1, st.CopiedExternal)
	require.Equal(t, 1, st.Downloaded)
	require.Equal(t, 3, st.Scanned)
	require.Equal(t, stopGenesis, st.StopReason)
}

type testAd struct {
	cid      cid.Cid
	provider peer.ID
}

func localStore(t *testing.T) config.StoreConfig {
	t.Helper()
	return config.StoreConfig{
		Compress: carstore.Gzip,
		Config: filestore.Config{
			Type: "local",
			Local: filestore.LocalConfig{
				BasePath: t.TempDir(),
			},
		},
	}
}

func rwMirror(main config.StoreConfig, ext ...config.StoreConfig) config.Mirror {
	return config.Mirror{
		MainMode: config.MainModeReadWrite,
		Main:     main,
		External: ext,
	}
}

func mustStore(t *testing.T, cfg config.StoreConfig) filestore.Interface {
	t.Helper()
	s, err := filestore.MakeFilestore(cfg.Config)
	require.NoError(t, err)
	return s
}

func writeCAR(t *testing.T, ds datastore.Batching, storeCfg config.StoreConfig, adCid cid.Cid) {
	t.Helper()
	fs := mustStore(t, storeCfg)
	w, err := carstore.NewWriter(ds, fs, carstore.WithCompress(storeCfg.Compress))
	require.NoError(t, err)
	_, err = w.Write(context.Background(), adCid, false, false)
	require.NoError(t, err)
}

func storeAdChain(t *testing.T, ds datastore.Datastore, chunksPerAd int) (latest, prev testAd) {
	t.Helper()
	a1 := storeAd(t, ds, chunksPerAd, nil)
	a2 := storeAd(t, ds, chunksPerAd, cidlink.Link{Cid: a1.cid})
	return a2, a1
}

func storeAdChain3(t *testing.T, ds datastore.Datastore) (ad3, ad2, ad1 testAd) {
	t.Helper()
	ad1 = storeAd(t, ds, 2, nil)
	ad2 = storeAd(t, ds, 2, cidlink.Link{Cid: ad1.cid})
	ad3 = storeAd(t, ds, 2, cidlink.Link{Cid: ad2.cid})
	return ad3, ad2, ad1
}

func storeAd(t *testing.T, ds datastore.Datastore, chunkCount int, prev ipld.Link) testAd {
	t.Helper()
	lsys := mkLinkSystem(ds)
	p, priv, _ := random.Identity()
	adv := &schema.Advertisement{
		Provider:   p.String(),
		Addresses:  []string{"/ip4/127.0.0.1/tcp/9999"},
		ContextID:  []byte("test-context-id"),
		Metadata:   []byte("test-metadata"),
		PreviousID: prev,
	}
	if chunkCount == 0 {
		adv.Entries = schema.NoEntries
	} else {
		adv.Entries, _ = newEntryList(t, lsys, chunkCount)
	}
	require.NoError(t, adv.Sign(priv))
	node, err := adv.ToNode()
	require.NoError(t, err)
	lnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	return testAd{cid: lnk.(cidlink.Link).Cid, provider: p}
}

func newEntryList(t *testing.T, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash) {
	t.Helper()
	var out []multihash.Multihash
	var next ipld.Link
	rnd := random.New()
	for range size {
		mhs := rnd.Multihashes(4)
		chunk := &schema.EntryChunk{Entries: mhs, Next: next}
		node, err := chunk.ToNode()
		require.NoError(t, err)
		lnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
		out = append(out, mhs...)
		next = lnk
	}
	return next, out
}

func mkLinkSystem(ds datastore.Datastore) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func cloneDS(t *testing.T, src datastore.Datastore) datastore.Batching {
	t.Helper()
	dst := dssync.MutexWrap(datastore.NewMapDatastore())
	res, err := src.Query(context.Background(), query.Query{})
	require.NoError(t, err)
	defer func() { _ = res.Close() }()
	for r := range res.Next() {
		require.NoError(t, r.Error)
		require.NoError(t, dst.Put(context.Background(), datastore.NewKey(r.Key), r.Value))
	}
	return dst
}
