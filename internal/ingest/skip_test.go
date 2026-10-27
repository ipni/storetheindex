package ingest

import (
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/config"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestSkippedAdDataRemovedWithMirror(t *testing.T) {
	// With a writable mirror, synced chunks stay in dsTmp until the ad is
	// mirrored. The root chunk links to a chunk the publisher does not have,
	// so the ad is skipped and not mirrored, and its data is removed.
	notFound := func(teo *testEnvOpts) {
		teo.publisherLinkSysFn = func(ds datastore.Batching) ipld.LinkSystem {
			lsys := mkProvLinkSystem(ds)
			read := lsys.StorageReadOpener
			// The publisher answers ipld.ErrNotExists with 404 Not Found
			lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
				r, err := read(lc, l)
				if errors.Is(err, datastore.ErrNotFound) {
					return nil, ipld.ErrNotExists{}
				}
				return r, err
			}
			return lsys
		}
	}
	for name, rootMhs := range map[string][]multihash.Multihash{
		"root with multihashes": random.Multihashes(1),
		"empty root":            nil,
	} {
		t.Run(name, func(t *testing.T) {
			te, mainDir := setupMirrorTestEnv(t, notFound)
			missing := cidlink.Link{Cid: random.Cids(1)[0]}
			rootLink, err := te.publisherLinkSys.Store(ipld.LinkContext{}, schema.Linkproto, entryChunkNode(t, rootMhs, missing))
			require.NoError(t, err)
			adCid := storeTestAd(t, te, rootLink, nil)
			syncTestAd(t, te, adCid)

			state, err := te.ingester.GetAdState(t.Context(), adCid)
			require.NoError(t, err)
			require.True(t, state.Skipped)
			require.Contains(t, state.SkipReason, string(adIngestContentNotFound))
			if len(rootMhs) != 0 {
				requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), rootMhs)
			}

			for _, c := range []cid.Cid{adCid, rootLink.(cidlink.Link).Cid} {
				has, err := te.ingester.dsTmp.Has(t.Context(), datastore.NewKey(c.String()))
				require.NoError(t, err)
				require.False(t, has, c)
			}
			require.NoFileExists(t, filepath.Join(mainDir, te.ingester.mirror.mainCarWriter.CarPath(adCid)))
		})
	}
}

// setupMirrorTestEnv returns a test environment whose ingester reads and writes
// a local Main CAR mirror, and the mirror directory. The options are applied
// before the mirror configuration.
func setupMirrorTestEnv(t *testing.T, opts ...func(*testEnvOpts)) (*testEnv, string) {
	mainDir := t.TempDir()
	cfg := defaultTestIngestConfig
	cfg.AdvertisementMirror.MainMode = config.MainModeReadWrite
	cfg.AdvertisementMirror.Main.Type = "local"
	cfg.AdvertisementMirror.Main.Local.BasePath = mainDir
	te := setupTestEnv(t, true, append(opts, func(optCfg *testEnvOpts) {
		optCfg.ingestConfig = &cfg
	})...)
	return te, mainDir
}

func entryChunkNode(t *testing.T, mhs []multihash.Multihash, next ipld.Link) ipld.Node {
	node, err := (&schema.EntryChunk{Entries: mhs, Next: next}).ToNode()
	require.NoError(t, err)
	return node
}

func newTestAd(t *testing.T, te *testEnv, entries, prev ipld.Link) schema.Advertisement {
	ad := schema.Advertisement{
		Provider:   te.pubHost.ID().String(),
		Addresses:  []string{"/ip4/127.0.0.1/tcp/9999"},
		Entries:    entries,
		ContextID:  []byte("test-context-id"),
		Metadata:   []byte("test-metadata"),
		PreviousID: prev,
	}
	require.NoError(t, ad.Sign(te.publisherPriv))
	return ad
}

// storeTestAd signs and stores an advertisement at the publisher
func storeTestAd(t *testing.T, te *testEnv, entries, prev ipld.Link) cid.Cid {
	node, err := newTestAd(t, te, entries, prev).ToNode()
	require.NoError(t, err)
	lnk, err := te.publisherLinkSys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	return lnk.(cidlink.Link).Cid
}

func syncTestAd(t *testing.T, te *testEnv, adCid cid.Cid) {
	te.publisher.SetRoot(adCid)
	_, err := te.ingester.Sync(t.Context(), testPeerInfo(te), 0, false, cid.Undef)
	require.NoError(t, err)
}

func testPeerInfo(te *testEnv) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
}
