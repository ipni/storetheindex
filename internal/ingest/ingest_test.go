package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/test/util"
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
)

const (
	testProtocolID    = 0x300000
	testRetryInterval = 2 * time.Second
	testRetryTimeout  = 10 * time.Second
)

var ingestCfg = config.Ingest{
	PubSubTopic: "test/ingest",
}

func randomAdvChain(priv crypto.PrivKey, lsys ipld.LinkSystem, numberOfMhChunksInEachAdv []uint8) (schema.Link_Advertisement, error) {
	var prevAdvLink schema.Link_Advertisement
	p, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	for i, numberOfMhsChunks := range numberOfMhChunksInEachAdv {
		r := rand.New(rand.NewSource(int64(numberOfMhsChunks)))
		mhsLnk, mhs, err := newRandomLinkedListWithErr(r, lsys, int(numberOfMhsChunks))
		if err != nil {
			return nil, err
		}

		ctxID := []byte("test-context-id" + fmt.Sprint(i))
		metadata := v0.Metadata{
			ProtocolID: testProtocolID,
			Data:       mhs[0],
		}
		addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
		_, prevAdvLink, err = schema.NewAdvertisementWithLink(lsys, priv, prevAdvLink, mhsLnk, ctxID, metadata, false, p.String(), addrs)
		if err != nil {
			return nil, err
		}
	}

	return prevAdvLink, nil
}

func TestSubscribeManyAdvs(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost := mkTestHost()
	// i, core, reg := mkIngest(t, h)
	i, core, _ := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub, lsys := mkMockPublisher(t, pubHost, srcStore)
	defer pub.Close()
	connectHosts(t, h, pubHost)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	quick.Check(func(numberOfMhChunksInEachAdv []uint8) bool {
		ctx := context.Background()
		fmt.Println("numberOfMhChunksInEachAdv:", numberOfMhChunksInEachAdv)
		advLink, err := randomAdvChain(priv, lsys, numberOfMhChunksInEachAdv)
		if err != nil {
			return false
		}

		pub.UpdateRoot(ctx, advLink.ToCid())
		headChan, err := i.Sync(ctx, pubHost.ID(), pubHost.Addrs()[0])
		if err != nil {
			return false
		}

		head := <-headChan
		time.Sleep(5 * time.Second)
		fmt.Println(advLink, head)
		return true
	}, &quick.Config{
		MaxCount: 3,
	})

}

func TestSubscribe(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost := mkTestHost()
	i, core, reg := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub, lsys := mkMockPublisher(t, pubHost, srcStore)
	defer pub.Close()
	connectHosts(t, h, pubHost)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

	// Test with two random advertisement publications.
	_, mhs, providerID := publishRandomAdv(t, i, pubHost, pub, lsys, false)

	// Check that the mhs have been indexed correctly.  Check providerID, not
	// pupHost.ID(), since provider is what was put into advertisement.
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)
	_, mhs, providerID = publishRandomAdv(t, i, pubHost, pub, lsys, false)
	// Check that the mhs have been indexed correctly.
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)

	// Test advertisement with fake signature of them.
	_, mhs, _ = publishRandomAdv(t, i, pubHost, pub, lsys, true)
	// No mhs should have been saved for related index
	for x := range mhs {
		_, b, _ := i.indexer.Get(mhs[x])
		require.False(t, b)
	}

	reg.BlockPeer(pubHost.ID())

	// Check that no advertisement is retrieved from publisher once it is no
	// longer allowed.
	c, _, _ := publishRandomIndexAndAdv(t, pub, lsys, false)
	adv, err := i.ds.Get(context.Background(), datastore.NewKey(c.String()))
	require.Error(t, err, datastore.ErrNotFound)
	require.Nil(t, adv)
}

func TestSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost := mkTestHost()
	i, core, _ := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub, lsys := mkMockPublisher(t, pubHost, srcStore)
	defer pub.Close()
	connectHosts(t, h, pubHost)

	c1, mhs, providerID := publishRandomIndexAndAdv(t, pub, lsys, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The explicit sync will happen concurrently with the sycn triggered by
	// the published advertisement.  These will be serialized in the go-legs
	// handler for the provider.
	end, err := i.Sync(ctx, pubHost.ID(), nil)
	require.NoError(t, err)
	select {
	case m := <-end:
		// We receive the CID that we synced.
		require.True(t, bytes.Equal(c1.Hash(), m))
		// Check that subscriber recorded latest sync.
		lnk := i.sub.GetLatestSync(pubHost.ID())
		lcid := lnk.(cidlink.Link).Cid
		require.Equal(t, lcid, c1)
		// Check that latest sync recorded in datastore
		lcid, err = i.getLatestSync(pubHost.ID())
		require.NoError(t, err)
		require.Equal(t, lcid, c1)
	case <-ctx.Done():
		t.Fatal("sync timeout")
	}
	// Checking providerID, since that was what was put in the advertisement, not pubhost.ID()
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)
}

func TestMultiplePublishers(t *testing.T) {
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost1 := mkTestHost()
	pubHost2 := mkTestHost()
	i, core, _ := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub1, lsys1 := mkMockPublisher(t, pubHost1, srcStore1)
	defer pub1.Close()
	pub2, lsys2 := mkMockPublisher(t, pubHost2, srcStore2)
	defer pub2.Close()

	// connect both providers
	connectHosts(t, h, pubHost1)
	connectHosts(t, h, pubHost2)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

	// Test with two random advertisement publications for each of them.
	c1, mhs, providerID := publishRandomAdv(t, i, pubHost1, pub1, lsys1, false)
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)
	c2, mhs, providerID := publishRandomAdv(t, i, pubHost2, pub2, lsys2, false)
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)

	// Check that subscriber recorded latest sync.
	lnk := i.sub.GetLatestSync(pubHost1.ID())
	lcid := lnk.(cidlink.Link).Cid
	require.Equal(t, lcid, c1)
	lnk = i.sub.GetLatestSync(pubHost2.ID())
	lcid = lnk.(cidlink.Link).Cid
	require.Equal(t, lcid, c2)

	lcid, err := i.getLatestSync(pubHost1.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c1)
	lcid, err = i.getLatestSync(pubHost2.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c2)
}

func mkTestHost() host.Host {
	h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	return h
}

// Make new indexer engine
func mkIndexer(t *testing.T, withCache bool) *engine.Engine {
	valueStore, err := storethehash.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return engine.New(resultCache, valueStore)
}

func mkRegistry(t *testing.T) *registry.Registry {
	discoveryCfg := config.Discovery{
		Policy: config.Policy{
			Allow: true,
			Trust: true,
		},
		PollInterval:   config.Duration(time.Minute),
		RediscoverWait: config.Duration(time.Minute),
	}
	reg, err := registry.NewRegistry(context.Background(), discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func mkProvLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, dsKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, dsKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}
func mkMockPublisher(t *testing.T, h host.Host, store datastore.Batching) (legs.Publisher, ipld.LinkSystem) {
	lsys := mkProvLinkSystem(store)
	ls, err := dtsync.NewPublisher(h, store, lsys, ingestCfg.PubSubTopic)
	require.NoError(t, err)
	return ls, lsys
}

func mkIngest(t *testing.T, h host.Host) (*Ingester, *engine.Engine, *registry.Registry) {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	reg := mkRegistry(t)
	core := mkIndexer(t, true)
	ing, err := NewIngester(ingestCfg, h, core, reg, store)
	require.NoError(t, err)
	return ing, core, reg
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
	mhs := util.RandomMultihashes(10)
	out = append(out, mhs...)
	nextLnk, _, err := schema.NewLinkedListOfMhs(lsys, mhs, nil)
	require.NoError(t, err)
	for i := 1; i < size; i++ {
		mhs := util.RandomMultihashes(10)
		nextLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, nextLnk)
		require.NoError(t, err)
		out = append(out, mhs...)
	}
	return nextLnk, out
}

func newRandomLinkedListWithErr(rand *rand.Rand, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash, error) {
	out := []multihash.Multihash{}
	mhs := util.RandomMultihashesFromRand(10, rand)
	out = append(out, mhs...)
	nextLnk, _, err := schema.NewLinkedListOfMhs(lsys, mhs, nil)
	if err != nil {
		return nil, nil, err
	}

	for i := 1; i < size; i++ {
		if i != 1 {
			// Dupes
			mhs = util.RandomMultihashesFromRand(0, rand)
		}
		nextLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, nextLnk)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, mhs...)
	}
	return nextLnk, out, err
}

func publishRandomIndexAndAdv(t *testing.T, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash, peer.ID) {
	mhs := util.RandomMultihashes(1)
	priv, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	p, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	ctxID := []byte("test-context-id")
	metadata := v0.Metadata{
		ProtocolID: testProtocolID,
		Data:       mhs[0],
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	mhsLnk, mhs := newRandomLinkedList(t, lsys, 3)
	_, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, mhsLnk, ctxID, metadata, false, p.String(), addrs)
	if fakeSig {
		_, advLnk, err = schema.NewAdvertisementWithFakeSig(lsys, priv, nil, mhsLnk, ctxID, metadata, false, p.String(), addrs)
	}
	require.NoError(t, err)
	lnk, err := advLnk.AsLink()
	require.NoError(t, err)
	err = pub.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	return lnk.(cidlink.Link).Cid, mhs, p
}

func checkMhsIndexedEventually(t *testing.T, ix *engine.Engine, p peer.ID, mhs []multihash.Multihash) {
	requireTrueEventually(t, func() bool { return providesAll(t, ix, p, mhs...) }, testRetryInterval, testRetryTimeout, "multihashes were not indexed")
}

func providesAll(t *testing.T, ix *engine.Engine, p peer.ID, mhs ...multihash.Multihash) bool {
	for _, mh := range mhs {
		values, exists, err := ix.Get(mh)
		if err != nil || !exists {
			t.Logf("err: %v, exists: %v", err, exists)
			return false
		}
		var found bool
		for _, v := range values {
			if v.ProviderID == p {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func publishRandomAdv(t *testing.T, i *Ingester, pubHost host.Host, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash, peer.ID) {
	c, mhs, providerID := publishRandomIndexAndAdv(t, pub, lsys, fakeSig)

	if !fakeSig {
		requireTrueEventually(t, func() bool {
			has, err := i.ds.Has(context.Background(), datastore.NewKey(c.String()))
			return err == nil && has
		}, 2*time.Second, 15*time.Second, "expected advertisement with ID %s was not received", c)
	}

	// Check if advertisement in datastore.
	adv, err := i.ds.Get(context.Background(), datastore.NewKey(c.String()))
	if !fakeSig {
		require.NoError(t, err, "err getting %s", c.String())
		require.NotNil(t, adv)
	} else {
		// If the signature is invalid is should not be stored.
		require.Nil(t, adv)
	}
	if !fakeSig {
		lnk := i.sub.GetLatestSync(pubHost.ID())
		lcid := lnk.(cidlink.Link).Cid
		require.Equal(t, lcid, c)
	}

	// Check if latest sync updated.
	lcid, err := i.getLatestSync(pubHost.ID())
	require.NoError(t, err)

	// If fakeSig Cids should not be saved.
	if !fakeSig {
		require.Equal(t, lcid, c)
	}
	return c, mhs, providerID
}

func requireTrueEventually(t *testing.T, attempt func() bool, interval time.Duration, timeout time.Duration, msgAndArgs ...interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if attempt() {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "timed out awaiting eventual success", msgAndArgs...)
			return
		case <-ticker.C:
		}
	}
}
