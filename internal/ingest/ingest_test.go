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
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/utils"
	pclient "github.com/filecoin-project/storetheindex/providerclient"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multihash"
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
	time.Sleep(2 * time.Second)

	t.Cleanup(func() {
		lp.Close()
		i.Close(context.Background())
	})

	// Test with two random advertisement publications.
	_, mhs := publishRandomAdv(t, i, lph, lp, lsys, false)
	// Check that the mhs have been indexed correctly.
	i.checkMhsIndexed(t, lph.ID(), mhs)
	_, mhs = publishRandomAdv(t, i, lph, lp, lsys, false)
	// Check that the mhs have been indexed correctly.
	i.checkMhsIndexed(t, lph.ID(), mhs)

	// Test advertisement with fake signature
	// of them.
	_, mhs = publishRandomAdv(t, i, lph, lp, lsys, true)
	// No mhs should have been saved for related index
	for x := range mhs {
		_, b, _ := i.indexer.Get(mhs[x])
		require.False(t, b)
	}

	err = i.Unsubscribe(context.Background(), lph.ID())
	require.NoError(t, err)

	// Check that no advertisement is retrieved from
	// peer once it has been unsubscribed.
	c, _ := publishRandomIndexAndAdv(t, lp, lsys, false)
	adv, err := i.ds.Get(datastore.NewKey(c.String()))
	require.Error(t, err, datastore.ErrNotFound)
	require.Nil(t, adv)

	// Unsubscribing twice shouldn't break anything.
	err = i.Unsubscribe(context.Background(), lph.ID())
	require.NoError(t, err)
}

func TestSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	lph := mkTestHost()
	i := mkIngest(t, h)
	lp, lsys := mkMockPublisher(t, lph, srcStore)

	connectHosts(t, h, lph)

	// Publish an advertisement without
	c1, mhs := publishRandomIndexAndAdv(t, lp, lsys, false)
	// Set mockClient in ingester with latest Cid to avoid trying to contact
	// a real provider.
	i.newClient = func(ctx context.Context, h host.Host, p peer.ID) (pclient.Provider, error) {
		return newMockClient(c1), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	end, err := i.Sync(ctx, lph.ID())
	t.Cleanup(func() {
		cancel()
		lp.Close()
		i.Close(context.Background())
	})
	require.NoError(t, err)
	select {
	case m := <-end:
		// We receive the CID that we synced.
		require.True(t, bytes.Equal([]byte(c1.Hash()), []byte(m)))
		i.checkMhsIndexed(t, lph.ID(), mhs)
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
	time.Sleep(2 * time.Second)

	t.Cleanup(func() {
		lp1.Close()
		lp2.Close()
		i.Close(context.Background())
	})

	// Test with two random advertisement publications for each
	// of them.
	c1, mhs := publishRandomAdv(t, i, lph1, lp1, lsys1, false)
	i.checkMhsIndexed(t, lph1.ID(), mhs)
	c2, mhs := publishRandomAdv(t, i, lph2, lp2, lsys2, false)
	i.checkMhsIndexed(t, lph2.ID(), mhs)

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

func mkRegistry(t *testing.T) *providers.Registry {
	discoveryCfg := config.Discovery{
		Policy: config.Policy{
			Allow: true,
			Trust: true,
		},
		PollInterval:   config.Duration(time.Minute),
		RediscoverWait: config.Duration(time.Minute),
	}
	reg, err := providers.NewRegistry(discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reg
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
	i, err := NewLegIngester(context.Background(), ingestCfg, h, mkIndexer(t, true), mkRegistry(t), store)
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

func newRandomLinkedList(t *testing.T, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash) {
	out := []multihash.Multihash{}
	mhs, _ := utils.RandomMultihashes(10)
	out = append(out, mhs...)
	nextLnk, _, err := schema.NewLinkedListOfMhs(lsys, mhs, nil)
	require.NoError(t, err)
	for i := 1; i < size; i++ {
		mhs, _ := utils.RandomMultihashes(10)
		nextLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, nextLnk)
		require.NoError(t, err)
		out = append(out, mhs...)
	}
	return nextLnk, out
}

func publishRandomIndexAndAdv(t *testing.T, pub legs.LegPublisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash) {
	mhs, _ := utils.RandomMultihashes(1)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	val := indexer.MakeValue(p, 0, mhs[0])
	mhsLnk, mhs := newRandomLinkedList(t, lsys, 3)
	_, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, mhsLnk, val.Metadata, false, p.String(), addrs)
	if fakeSig {
		_, advLnk, err = schema.NewAdvertisementWithFakeSig(lsys, priv, nil, mhsLnk, val.Metadata, false, p.String(), addrs)
	}
	require.NoError(t, err)
	lnk, err := advLnk.AsLink()
	require.NoError(t, err)
	err = pub.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	return lnk.(cidlink.Link).Cid, mhs
}

func (i *legIngester) checkMhsIndexed(t *testing.T, p peer.ID, mhs []multihash.Multihash) {
	for x := range mhs {
		v, b, err := i.indexer.Get(mhs[x])
		require.NoError(t, err)
		require.True(t, b)
		require.Equal(t, v[0].ProviderID, p)
	}
}
func publishRandomAdv(t *testing.T, i *legIngester, lph host.Host, lp legs.LegPublisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash) {
	c, mhs := publishRandomIndexAndAdv(t, lp, lsys, fakeSig)

	// TODO: fix this - do not rely on sleep time
	// Give some time for the advertisement to propagate
	time.Sleep(2 * time.Second)

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
	return c, mhs
}

// Implementation of a mock provider client.
var _ pclient.Provider = &mockClient{}

type mockClient struct {
	cid.Cid
}

func newMockClient(c cid.Cid) *mockClient {
	return &mockClient{c}
}
func (c *mockClient) GetAdv(ctx context.Context, id cid.Cid) (*pclient.AdResponse, error) {
	return nil, nil
}

func (c *mockClient) GetLatestAdv(ctx context.Context) (*pclient.AdResponse, error) {
	return &pclient.AdResponse{ID: c.Cid}, nil
}

func (c *mockClient) Close() error {
	return nil
}
