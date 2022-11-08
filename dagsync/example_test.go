package dagsync_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/filecoin-project/storetheindex/dagsync"
	"github.com/filecoin-project/storetheindex/dagsync/dtsync"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multicodec"
)

var srcHost host.Host

func ExamplePublisher() {
	// Init dagsync publisher and subscriber.
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, _ = libp2p.New()
	srcLnkS := makeLinkSystem(srcStore)

	pub, err := dtsync.NewPublisher(srcHost, srcStore, srcLnkS, testTopic)
	if err != nil {
		panic(err)
	}
	defer pub.Close()

	// Update root on publisher one with item
	itm1 := basicnode.NewString("hello world")
	lnk1, err := store(srcStore, itm1)
	if err != nil {
		panic(err)
	}
	if err = pub.UpdateRoot(context.Background(), lnk1.(cidlink.Link).Cid); err != nil {
		panic(err)
	}
	log.Print("Publish 1:", lnk1.(cidlink.Link).Cid)

	// Update root on publisher one with item
	itm2 := basicnode.NewString("hello world 2")
	lnk2, err := store(srcStore, itm2)
	if err != nil {
		panic(err)
	}
	if err = pub.UpdateRoot(context.Background(), lnk2.(cidlink.Link).Cid); err != nil {
		panic(err)
	}
	log.Print("Publish 2:", lnk2.(cidlink.Link).Cid)
}

func ExampleSubscriber() {
	dstHost, _ := libp2p.New()

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkSys := makeLinkSystem(dstStore)

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)

	sub, err := dagsync.NewSubscriber(dstHost, dstStore, dstLnkSys, "/indexer/ingest/testnet", nil)
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	// Connections must be made after Subscriber is created, because the
	// gossip pubsub must be created before connections are made.  Otherwise,
	// the connecting hosts will not see the destination host has pubsub and
	// messages will not get published.
	dstPeerInfo := dstHost.Peerstore().PeerInfo(dstHost.ID())
	if err = srcHost.Connect(context.Background(), dstPeerInfo); err != nil {
		panic(err)
	}

	watcher, cancelWatcher := sub.OnSyncFinished()
	defer cancelWatcher()

	for syncFin := range watcher {
		fmt.Println("Finished sync to", syncFin.Cid, "with peer:", syncFin.PeerID)
	}
}

func makeLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(lnk.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			return ds.Put(lctx.Ctx, datastore.NewKey(lnk.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func store(srcStore datastore.Batching, n ipld.Node) (ipld.Link, error) {
	linkproto := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}
	lsys := makeLinkSystem(srcStore)

	return lsys.Store(ipld.LinkContext{}, linkproto, n)
}
