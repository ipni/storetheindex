package head_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/storetheindex/dagsync/p2p/protocol/head"
	"github.com/ipni/storetheindex/dagsync/test"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	multistream "github.com/multiformats/go-multistream"
	"github.com/stretchr/testify/require"
)

const testTopic = "/testtopic"

func TestFetchLatestHead(t *testing.T) {
	const ipPrefix = "/ip4/127.0.0.1/tcp/"

	publisher := test.MkTestHost()
	client := test.MkTestHost()

	var addrs []multiaddr.Multiaddr
	for _, a := range publisher.Addrs() {
		// Change /ip4/127.0.0.1/tcp/<port> to /dns4/localhost/tcp/<port> to
		// test that dns address works.
		if strings.HasPrefix(a.String(), ipPrefix) {
			addrStr := "/dns4/localhost/tcp/" + a.String()[len(ipPrefix):]
			addr, err := multiaddr.NewMultiaddr(addrStr)
			require.NoError(t, err)
			addrs = append(addrs, addr)
			break
		}
	}

	// Provide multiaddrs to connect to
	client.Peerstore().AddAddrs(publisher.ID(), addrs, time.Hour)

	publisherStore := dssync.MutexWrap(datastore.NewMapDatastore())
	rootLnk, err := test.Store(publisherStore, basicnode.NewString("hello world"))
	require.NoError(t, err)

	p := head.NewPublisher()
	go p.Serve(publisher, testTopic)
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Check for expected error when querying with bad topic.
	_, err = head.QueryRootCid(ctx, client, "/badtopic", publisher.ID())
	var errNoSupport multistream.ErrNotSupported[protocol.ID]
	require.ErrorAs(t, err, &errNoSupport)

	c, err := head.QueryRootCid(ctx, client, testTopic, publisher.ID())
	require.NoError(t, err)
	require.Equal(t, cid.Undef, c, "Expected cid undef because there is no root")

	err = p.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid)
	require.NoError(t, err)

	c, err = head.QueryRootCid(ctx, client, testTopic, publisher.ID())
	require.NoError(t, err)

	require.Equal(t, rootLnk.(cidlink.Link).Cid, c)
}

func TestOldProtocolID(t *testing.T) {
	publisher := test.MkTestHost()
	client := test.MkTestHost()

	// Provide multiaddrs to connect to
	client.Peerstore().AddAddrs(publisher.ID(), publisher.Addrs(), time.Hour)

	publisherStore := dssync.MutexWrap(datastore.NewMapDatastore())
	rootLnk, err := test.Store(publisherStore, basicnode.NewString("hello world"))
	require.NoError(t, err)

	p := head.NewPublisher()
	go p.ServePrevious(publisher, testTopic)
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = p.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid)
	require.NoError(t, err)

	c, err := head.QueryRootCid(ctx, client, testTopic, publisher.ID())
	require.NoError(t, err)

	require.Equal(t, rootLnk.(cidlink.Link).Cid, c)
}
