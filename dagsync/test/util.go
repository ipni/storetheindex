package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipni/storetheindex/dagsync/httpsync/maconv"
	"github.com/ipni/storetheindex/dagsync/p2p/protocol/head"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

const (
	waitForMeshTimeout = 10 * time.Second
	publishTimeout     = 100 * time.Millisecond
)

func WaitForMeshWithMessage(t *testing.T, topic string, hosts ...host.Host) []*pubsub.Topic {
	retries := 5
	for {
		topics := waitForMeshWithMessage(t, topic, hosts...)
		if topics != nil {
			return topics
		}
		retries--
		msg := "Mesh failed to startup"
		require.NotZero(t, retries, msg)
		t.Log(msg + " retrying")
	}
}

// WaitForMeshWithMessage sets up a gossipsub network and sends a test message.
// Blocks until all other hosts see the first host's message.
func waitForMeshWithMessage(t *testing.T, topic string, hosts ...host.Host) []*pubsub.Topic {
	now := time.Now()
	meshFormed := false
	defer func() {
		if meshFormed {
			t.Log("Mesh formed in", time.Since(now))
		}
	}()

	addrInfos := make([]peer.AddrInfo, len(hosts))
	for i, h := range hosts {
		addrInfos[i] = *host.InfoFromHost(h)
	}

	for _, h := range hosts {
		for _, addrInfo := range addrInfos {
			if h.ID() == addrInfo.ID {
				continue
			}
			h.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, time.Hour)
			err := h.Connect(context.Background(), addrInfo)
			require.NoError(t, err, "Failed to connect")
		}
	}

	pubsubs := make([]*pubsub.PubSub, len(hosts))
	topics := make([]*pubsub.Topic, len(hosts))
	for i, h := range hosts {
		addrInfosWithoutSelf := make([]peer.AddrInfo, 0, len(addrInfos)-1)
		for _, ai := range addrInfos {
			if ai.ID != h.ID() {
				addrInfosWithoutSelf = append(addrInfosWithoutSelf, ai)
			}
		}

		pubsub, err := pubsub.NewGossipSub(context.Background(), h, pubsub.WithDirectPeers(addrInfosWithoutSelf))
		require.NoError(t, err, "Failed to start gossipsub")

		tpc, err := pubsub.Join(topic)
		require.NoError(t, err, "Failed to join topic")

		pubsubs[i] = pubsub
		topics[i] = tpc
	}

	require.NotEqual(t, 1, len(pubsubs),
		"No point in using this helper if there's only one host. Did you mean to pass in another host?")

	restTopics := topics[1:]
	wg := sync.WaitGroup{}

	for i := range restTopics {
		wg.Add(1)

		s, err := restTopics[i].Subscribe()
		require.NoError(t, err, "Failed to subscribe")

		go func(s *pubsub.Subscription) {
			_, err := s.Next(context.Background())
			require.NoError(t, err, "Failed in waiting for startupCheck msg in goroutine")
			wg.Done()

			// Wait until someone else picks up this topic and sends a message before
			// we cancel. This way the topic isn't unsubscribed to before we start
			// the test.
			_, err = s.Next(context.Background())
			require.NoError(t, err, "Error getting next message on subscription")
			s.Cancel()
		}(s)
	}

	done := make(chan (struct{}))
	go func() {
		wg.Wait()
		close(done)
	}()

	tpc := topics[0]
	err := tpc.Publish(context.Background(), []byte("hi"))
	require.NoError(t, err, "Failed to publish")

	timeout := time.NewTimer(waitForMeshTimeout)
	defer timeout.Stop()
	pubTimeout := time.NewTimer(publishTimeout)
	defer pubTimeout.Stop()

	// If not all subscribers get the msg, let's resend the message until they
	// get it or we timeout.
	for {
		select {
		case <-done:
			meshFormed = true
			return topics
		case <-timeout.C:
			return nil
		case <-pubTimeout.C:
			err := tpc.Publish(context.Background(), []byte("hi"))
			require.NoError(t, err, "Failed to publish")
			pubTimeout.Reset(publishTimeout)
		}
	}
}

// encode hardcodes some encoding choices for ease of use in fixture generation;
// just gimme a link and stuff the bytes in a map.
// (also return the node again for convenient assignment.)
func encode(lsys ipld.LinkSystem, n ipld.Node) (ipld.Node, ipld.Link) {
	lp := cidlink.LinkPrototype{
		Prefix: prefix,
	}

	lnk, err := lsys.Store(ipld.LinkContext{}, lp, n)
	if err != nil {
		panic(err)
	}
	return n, lnk
}

var prefix = cid.Prefix{
	Version:  1,
	Codec:    uint64(multicodec.DagJson),
	MhType:   uint64(multicodec.Sha2_256),
	MhLength: 16,
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

func MkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		val, err := ds.Get(context.Background(), datastore.NewKey(lnk.String()))
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

func Store(srcStore datastore.Batching, n ipld.Node) (ipld.Link, error) {
	linkproto := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}
	lsys := MkLinkSystem(srcStore)

	return lsys.Store(ipld.LinkContext{}, linkproto, n)
}

func MkTestHost(options ...libp2p.Option) host.Host {
	options = append(options, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	h, _ := libp2p.New(options...)
	return h
}

// Return the chain with all nodes or just half of it for testing
func MkChain(lsys ipld.LinkSystem, full bool) []ipld.Link {
	out := make([]ipld.Link, 4)
	_, leafAlphaLnk := encode(lsys, basicnode.NewString("alpha"))
	_, leafBetaLnk := encode(lsys, basicnode.NewString("beta"))
	_, middleMapNodeLnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 3, func(na fluent.MapAssembler) {
		na.AssembleEntry("foo").AssignBool(true)
		na.AssembleEntry("bar").AssignBool(false)
		na.AssembleEntry("nested").CreateMap(2, func(na fluent.MapAssembler) {
			na.AssembleEntry("alink").AssignLink(leafAlphaLnk)
			na.AssembleEntry("nonlink").AssignString("zoo")
		})
	}))
	_, middleListNodeLnk := encode(lsys, fluent.MustBuildList(basicnode.Prototype__List{}, 4, func(na fluent.ListAssembler) {
		na.AssembleValue().AssignLink(leafAlphaLnk)
		na.AssembleValue().AssignLink(leafAlphaLnk)
		na.AssembleValue().AssignLink(leafBetaLnk)
		na.AssembleValue().AssignLink(leafAlphaLnk)
	}))

	_, ch1Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("linkedList").AssignLink(middleListNodeLnk)
	}))
	out[3] = ch1Lnk
	_, ch2Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
		na.AssembleEntry("linkedMap").AssignLink(middleMapNodeLnk)
		na.AssembleEntry("ch1").AssignLink(ch1Lnk)
	}))
	out[2] = ch2Lnk
	if full {
		_, ch3Lnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("linkedString").AssignLink(leafAlphaLnk)
			na.AssembleEntry("ch2").AssignLink(ch2Lnk)
		}))
		out[1] = ch3Lnk
		_, headLnk := encode(lsys, fluent.MustBuildMap(basicnode.Prototype__Map{}, 4, func(na fluent.MapAssembler) {
			na.AssembleEntry("plain").AssignString("olde string")
			na.AssembleEntry("ch3").AssignLink(ch3Lnk)
		}))
		out[0] = headLnk
	}
	return out
}

type TestPublisher interface {
	// Addrs returns the addresses that the publisher is listening on.
	Addrs() []multiaddr.Multiaddr
	// ID returns the peer ID associated with the publisher.
	ID() peer.ID
}

func WaitForP2PPublisher(publisher TestPublisher, clientHost host.Host, topic string) error {
	const timeout = 10 * time.Second
	const addrTTL = 5*time.Second + timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Put the publisher's addresses into the client's peerstore so that the
	// client knows how to connect to the publisher.
	peerStore := clientHost.Peerstore()
	if peerStore != nil {
		peerStore.AddAddrs(publisher.ID(), publisher.Addrs(), addrTTL)
	}

	for ctx.Err() == nil {
		_, err := head.QueryRootCid(ctx, clientHost, topic, publisher.ID())
		if err == nil {
			// Publisher ready
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timeout waiting for publilsher")
}

func WaitForHttpPublisher(publisher TestPublisher) error {
	const timeout = 10 * time.Second
	headURL, err := maconv.ToURL(publisher.Addrs()[0])
	if err != nil {
		return err
	}
	headURL.Path = path.Join(headURL.Path, "head")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client := &http.Client{}
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, "GET", headURL.String(), nil)
		if err != nil {
			return err
		}

		resp, err := client.Do(req)
		if err == nil {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return fmt.Errorf("cannot read response body: %s", err)
			}
			if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
				return fmt.Errorf("publisher responded with error: %d %s", resp.StatusCode, body)
			}
			return nil
		}
	}
	return errors.New("timeout waiting for http publilsher")
}
