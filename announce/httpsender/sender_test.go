package httpsender_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/storetheindex/announce/httpsender"
	"github.com/ipni/storetheindex/announce/message"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	testPeerIDStr = "12D3KooWQ9j3Ur5V9U63Vi6ved72TcA3sv34k74W3wpW5rwNvDc3"
	testCidStr    = "QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB"
	testAddrStr   = "/ip4/127.0.0.1/tcp/9999"
	testAddrStr2  = "/dns4/localhost/tcp/1234"
)

var (
	testCid    cid.Cid
	testPeerID peer.ID
	testAddrs  []multiaddr.Multiaddr
)

func init() {
	var err error
	testPeerID, err = peer.Decode(testPeerIDStr)
	if err != nil {
		panic(err)
	}
	testCid, err = cid.Decode(testCidStr)
	if err != nil {
		panic(err)
	}
	testAddrs = make([]multiaddr.Multiaddr, 1)
	testAddrs[0], err = multiaddr.NewMultiaddr(testAddrStr)
	if err != nil {
		panic(err)
	}
}

func TestSend(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Decode CID and originator addresses from message.
		an := message.Message{}
		err := an.UnmarshalCBOR(r.Body)
		require.NoError(t, err)

		require.NotZero(t, len(an.Addrs), "must specify location to fetch on direct announcments")

		addrs, err := an.GetAddrs()
		require.NoError(t, err, "could not decode addrs from announce message")

		ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
		require.NoError(t, err)
		require.Equal(t, 1, len(ais), "peer id must be the same for all addresses")

		addrInfo := ais[0]
		require.Equal(t, testPeerID, addrInfo.ID)
		require.Equal(t, len(testAddrs), len(addrInfo.Addrs))
		require.True(t, addrInfo.Addrs[0].Equal(testAddrs[0]))

		count++
	}))
	defer ts.Close()

	announceURL, err := url.Parse(ts.URL + httpsender.DefaultAnnouncePath)
	require.NoError(t, err)

	_, err = httpsender.New(nil)
	require.Error(t, err)

	sender, err := httpsender.New([]*url.URL{announceURL}, httpsender.WithClient(ts.Client()))
	require.NoError(t, err)
	defer sender.Close()

	ai := peer.AddrInfo{
		ID:    testPeerID,
		Addrs: testAddrs,
	}
	addrs, err := peer.AddrInfoToP2pAddrs(&ai)
	require.NoError(t, err)
	msg := message.Message{
		Cid: testCid,
	}
	msg.SetAddrs(addrs)

	err = sender.Send(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, sender.Close())

	// Create sender with duplicate URLs.
	sender, err = httpsender.New([]*url.URL{announceURL, announceURL})
	require.NoError(t, err)
	defer sender.Close()

	count = 0
	err = sender.Send(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Test sending to multiple HTTP endpoints.
	var count2 int
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count2++
	}))
	defer ts2.Close()

	announceURL2, err := url.Parse(ts2.URL + httpsender.DefaultAnnouncePath)
	require.NoError(t, err)

	sender.Close()
	sender, err = httpsender.New([]*url.URL{announceURL, announceURL2})
	require.NoError(t, err)
	defer sender.Close()

	count = 0
	err = sender.Send(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, 1, count2)

	// Test double close.
	require.NoError(t, sender.Close())
	require.NoError(t, sender.Close())
}

func TestSendTimeout(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer ts.Close()
	defer close(block)

	announceURL, err := url.Parse(ts.URL + httpsender.DefaultAnnouncePath)
	require.NoError(t, err)

	sender, err := httpsender.New([]*url.URL{announceURL})
	require.NoError(t, err)
	defer sender.Close()

	ai := peer.AddrInfo{
		ID:    testPeerID,
		Addrs: testAddrs,
	}
	addrs, err := peer.AddrInfoToP2pAddrs(&ai)
	require.NoError(t, err)
	msg := message.Message{
		Cid: testCid,
	}
	msg.SetAddrs(addrs)

	// Test timeout of Send context.
	sendCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = sender.Send(sendCtx, msg)
	require.Truef(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "i/o timeout"), "error is %q", err)

	// Test client timeout.
	sender.Close()
	sender, err = httpsender.New([]*url.URL{announceURL}, httpsender.WithTimeout(time.Second))
	require.NoError(t, err)
	defer sender.Close()
	err = sender.Send(context.Background(), msg)
	require.Truef(t, strings.Contains(err.Error(), "Client.Timeout exceeded") || strings.Contains(err.Error(), "i/o timeout"), "error is %q", err)
}

func TestJSONSend(t *testing.T) {
	var count int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Decode CID and originator addresses from message.
		an := message.Message{}
		err := json.NewDecoder(r.Body).Decode(&an)
		require.NoError(t, err)

		require.NotZero(t, len(an.Addrs), "must specify location to fetch on direct announcments")

		addrs, err := an.GetAddrs()
		require.NoError(t, err, "could not decode addrs from announce message")

		ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
		require.NoError(t, err)
		require.Equal(t, 1, len(ais), "peer id must be the same for all addresses")

		addrInfo := ais[0]
		require.Equal(t, testPeerID, addrInfo.ID)
		require.Equal(t, len(testAddrs), len(addrInfo.Addrs))
		require.True(t, addrInfo.Addrs[0].Equal(testAddrs[0]))

		count++
	}))
	defer ts.Close()

	announceURL, err := url.Parse(ts.URL + httpsender.DefaultAnnouncePath)
	require.NoError(t, err)

	_, err = httpsender.New(nil)
	require.Error(t, err)

	sender, err := httpsender.New([]*url.URL{announceURL}, httpsender.WithClient(ts.Client()))
	require.NoError(t, err)
	defer sender.Close()

	ai := peer.AddrInfo{
		ID:    testPeerID,
		Addrs: testAddrs,
	}
	addrs, err := peer.AddrInfoToP2pAddrs(&ai)
	require.NoError(t, err)
	msg := message.Message{
		Cid: testCid,
	}
	msg.SetAddrs(addrs)

	err = sender.SendJson(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, sender.Close())
}
