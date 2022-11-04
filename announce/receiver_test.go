package announce_test

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/announce"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	testTopic        = "/announce/testtopic"
	testCidString    = "QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB"
	testCid2String   = "Qmejoony52NYREWv3e9Ap6Uvg29GmJKJpxaDgAbzzYL9kX"
	testPeerIDString = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"
	testAddrString   = "/ip4/127.0.0.1/tcp/9999"
)

var (
	testCid    cid.Cid
	testCid2   cid.Cid
	testPeerID peer.ID
	testAddrs  []multiaddr.Multiaddr
)

func init() {
	var err error
	testCid, err = cid.Decode(testCidString)
	if err != nil {
		panic(err)
	}
	testCid2, err = cid.Decode(testCid2String)
	if err != nil {
		panic(err)
	}

	testPeerID, err = peer.Decode(testPeerIDString)
	if err != nil {
		panic(err)
	}

	testAddrs = make([]multiaddr.Multiaddr, 1)
	testAddrs[0], err = multiaddr.NewMultiaddr(testAddrString)
	if err != nil {
		panic(err)
	}
}

func TestReceiverBasic(t *testing.T) {
	srcHost, _ := libp2p.New()
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	err = rcvr.Direct(context.Background(), testCid, testPeerID, testAddrs)
	require.NoError(t, err)

	amsg, err := rcvr.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, testPeerID, amsg.PeerID)

	require.NoError(t, rcvr.Close())
}

func TestReceiverCloseWaitingNext(t *testing.T) {
	srcHost, _ := libp2p.New()
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	// Test close while Next is waiting.
	errChan := make(chan error)
	go func() {
		_, nextErr := rcvr.Next(context.Background())
		errChan <- nextErr
	}()

	require.NoError(t, rcvr.Close())

	select {
	case err = <-errChan:
		require.ErrorIs(t, err, announce.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error return")
	}

}

func TestReceiverCloseWaitingDirect(t *testing.T) {
	srcHost, _ := libp2p.New()
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	// Test close while Direct is waiting.
	rcvr, err = announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		directErr := rcvr.Direct(context.Background(), testCid, testPeerID, testAddrs)
		if directErr != nil {
			errChan <- directErr
			return
		}
		errChan <- rcvr.Direct(context.Background(), testCid2, testPeerID, testAddrs)
	}()

	require.NoError(t, rcvr.Close())

	select {
	case err = <-errChan:
		require.ErrorIs(t, err, announce.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error return")
	}
}

func TestReceiverCidCache(t *testing.T) {
	srcHost, _ := libp2p.New()
	rcvr, err := announce.NewReceiver(srcHost, testTopic)
	require.NoError(t, err)

	err = rcvr.Direct(context.Background(), testCid, testPeerID, testAddrs)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	amsg, err := rcvr.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, testPeerID, amsg.PeerID)

	// Request another announce with the same CID.
	err = rcvr.Direct(context.Background(), testCid, testPeerID, testAddrs)
	require.NoError(t, err)

	// Next should not receive another announce.
	_, err = rcvr.Next(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, rcvr.Close())
}
