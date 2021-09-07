package ingest

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	ingestclient "github.com/filecoin-project/indexer-reference-provider/api/v0/client"
	"github.com/filecoin-project/indexer-reference-provider/api/v0/models"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-legs"
)

var ingestCfg = config.Ingest{
	PubSubTopic: "test/ingest",
}

var prefix = schema.Linkproto.Prefix

func TestSubscribe(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	lph := mkTestHost()
	i := mkIngest(t, h)
	lp, lsys := mkMockPublisher(t, lph, srcStore)

	connectHosts(t, h, lph)

	// Subscribe to provider
	err := i.Subscribe(context.Background(), lph.ID())
	require.NoError(t, err)
	require.NotNil(t, i.subs[lph.ID()])

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	t.Cleanup(func() {
		lp.Close()
		i.Close(context.Background())
	})

	// Test with two random advertisement publications.
	_, cids := publishRandomAdv(t, i, lph, lp, lsys, false)
	// Check that the cids have been indexed correctly.
	i.checkCidsIndexed(t, lph.ID(), cids)
	_, cids = publishRandomAdv(t, i, lph, lp, lsys, false)
	// Check that the cids have been indexed correctly.
	i.checkCidsIndexed(t, lph.ID(), cids)

	// Test advertisement with fake signature
	// of them.
	_, cids = publishRandomAdv(t, i, lph, lp, lsys, true)
	// No cids should have been saved for related index
	for x := range cids {
		_, b, _ := i.indexer.Get(cids[x])
		require.False(t, b)
	}

	i.Unsubscribe(context.Background(), lph.ID())
	// Check that no advertisement is retrieved from
	// peer once it has been unsubscribed.
	c, _ := publishRandomIndexAndAdv(t, lp, lsys, false)
	adv, err := i.ds.Get(datastore.NewKey(c.String()))
	require.Error(t, err, datastore.ErrNotFound)
	require.Nil(t, adv)

	// Unsubscribing twice shouldn't break anything.
	i.Unsubscribe(context.Background(), lph.ID())

}

func TestSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	lph := mkTestHost()
	i := mkIngest(t, h)
	lp, lsys := mkMockPublisher(t, lph, srcStore)

	connectHosts(t, h, lph)

	// Publish an advertisement without
	c1, cids := publishRandomIndexAndAdv(t, lp, lsys, false)
	// Set mockClient in ingester with latest Cid to avoid trying to contact
	// a real provider.
	i.client = newMockClient(c1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	end, err := i.Sync(ctx, lph.ID())
	t.Cleanup(func() {
		cancel()
		lp.Close()
		i.Close(context.Background())
	})
	require.NoError(t, err)
	select {
	case c := <-end:
		// We receive the CID that we synced.
		require.Equal(t, c1, c)
		i.checkCidsIndexed(t, lph.ID(), cids)
		lcid, err := i.getLatestSync(lph.ID())
		require.NoError(t, err)
		require.Equal(t, lcid, c1)
	case <-ctx.Done():
		t.Fatal("sync timeout")
	}

}

func TestMultipleSubscriptions(t *testing.T) {
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	lph1 := mkTestHost()
	lph2 := mkTestHost()
	i := mkIngest(t, h)
	lp1, lsys1 := mkMockPublisher(t, lph1, srcStore1)
	lp2, lsys2 := mkMockPublisher(t, lph2, srcStore2)

	// Subscribe to both providers
	connectHosts(t, h, lph1)
	err := i.Subscribe(context.Background(), lph1.ID())
	require.NoError(t, err)
	require.NotNil(t, i.subs[lph1.ID()])

	connectHosts(t, h, lph2)
	err = i.Subscribe(context.Background(), lph2.ID())
	require.NoError(t, err)
	require.NotNil(t, i.subs[lph2.ID()])

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	t.Cleanup(func() {
		lp1.Close()
		lp2.Close()
		i.Close(context.Background())
	})

	// Test with two random advertisement publications for each
	// of them.
	c1, cids := publishRandomAdv(t, i, lph1, lp1, lsys1, false)
	i.checkCidsIndexed(t, lph1.ID(), cids)
	c2, cids := publishRandomAdv(t, i, lph2, lp2, lsys2, false)
	i.checkCidsIndexed(t, lph2.ID(), cids)

	lcid, err := i.getLatestSync(lph1.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c1)
	lcid2, err := i.getLatestSync(lph2.ID())
	require.NoError(t, err)
	require.Equal(t, lcid2, c2)

}

func mkTestHost() host.Host {
	h, _ := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	return h
}

// Make new indexer engine
func mkIndexer(t *testing.T, withCache bool) *engine.Engine {
	var tmpDir string
	var err error
	if runtime.GOOS == "windows" {
		tmpDir, err = ioutil.TempDir("", "sth")
		if err != nil {
			t.Fatal(err)
		}
	} else {
		tmpDir = t.TempDir()
	}
	valueStore, err := storethehash.New(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return engine.New(resultCache, valueStore)
}

func mkProvLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(dsKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(dsKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}
func mkMockPublisher(t *testing.T, h host.Host, store datastore.Batching) (legs.LegPublisher, ipld.LinkSystem) {
	ctx := context.Background()
	lsys := mkProvLinkSystem(store)
	lt, err := legs.MakeLegTransport(context.Background(), h, store, lsys, ingestCfg.PubSubTopic)
	if err != nil {
		t.Fatal(err)
	}
	ls, err := legs.NewPublisher(ctx, lt)
	require.NoError(t, err)
	return ls, lsys
}

func mkIngest(t *testing.T, h host.Host) *legIngester {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	i, err := NewLegIngester(context.Background(), ingestCfg, h, mkIndexer(t, true), store)
	require.NoError(t, err)
	return i.(*legIngester)
}

func RandomCids(n int) ([]cid.Cid, error) {
	var prng = rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		res[i] = c
	}
	return res, nil
}

func connectHosts(t *testing.T, srcHost, dstHost host.Host) {
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
}

func publishRandomIndexAndAdv(t *testing.T, pub legs.LegPublisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []cid.Cid) {
	cids, _ := RandomCids(10)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	cidsLnk, err := schema.NewListOfCids(lsys, cids)
	require.NoError(t, err)
	_, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, cidsLnk, val.Metadata, false, p.String())
	if fakeSig {
		_, advLnk, err = schema.NewAdvertisementWithFakeSig(lsys, priv, nil, cidsLnk, val.Metadata, false, p.String())
	}
	require.NoError(t, err)
	lnk, err := advLnk.AsLink()
	require.NoError(t, err)
	err = pub.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	return lnk.(cidlink.Link).Cid, cids
}

func (i *legIngester) checkCidsIndexed(t *testing.T, p peer.ID, cids []cid.Cid) {
	for x := range cids {
		v, b, err := i.indexer.Get(cids[x])
		require.NoError(t, err)
		require.True(t, b)
		require.Equal(t, v[0].ProviderID, p)
	}
}
func publishRandomAdv(t *testing.T, i *legIngester, lph host.Host, lp legs.LegPublisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []cid.Cid) {
	c, cids := publishRandomIndexAndAdv(t, lp, lsys, fakeSig)

	// Give some time for the advertisement to propagate
	time.Sleep(500 * time.Millisecond)

	// Check if advertisement in datastore.
	adv, err := i.ds.Get(datastore.NewKey(c.String()))
	if !fakeSig {
		require.NoError(t, err)
		require.NotNil(t, adv)
	} else {
		// If the signature is invalid we shouldn't have store it.
		require.Nil(t, adv)
	}
	// Check if latest sync updated.
	lcid, err := i.getLatestSync(lph.ID())
	require.NoError(t, err)

	// If fakeSig Cids should not be saved.
	if !fakeSig {
		require.Equal(t, lcid, c)
	}
	return c, cids
}

// Implementation of a mock provider client.
var _ ingestclient.Provider = &mockClient{}

type mockClient struct {
	cid.Cid
}

func newMockClient(c cid.Cid) *mockClient {
	return &mockClient{c}
}
func (c *mockClient) GetAdv(ctx context.Context, p peer.ID, id cid.Cid) (*models.AdResponse, error) {
	return nil, nil
}

func (c *mockClient) GetLatestAdv(ctx context.Context, p peer.ID) (*models.AdResponse, error) {
	return &models.AdResponse{ID: c.Cid}, nil
}
