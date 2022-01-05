package libp2pclient

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"
)

// Client is responsible for sending requests and receiving responses to and
// from libp2p peers.  Each instance of Client communicates with a single peer
// using a single protocolID.
type Client struct {
	ctxLock ctxMutex
	host    host.Host
	peerID  peer.ID
	protoID protocol.ID
	r       msgio.ReadCloser
	stream  network.Stream
}

// DecodeResponseFunc is a function that is passed into this generic libp2p
// Client to decode a response message.  This is needed because the generic
// client cannot decode the response message since the message is of a type
// only know to a specific libp2p client using this generic client.
type DecodeResponseFunc func([]byte) error

const (
	// default port for libp2p client to connect to
	defaultLibp2pPort = 3003
	// Timeout to wait for a response after a request is sent
	readMessageTimeout = 10 * time.Second
)

// ErrReadTimeout is an error that occurs when no message is read within the
// timeout period
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// New creates a new libp2pclient Client that communicates with a specific peer identified by
// protocolID.  If host is nil, then one is created.
func New(p2pHost host.Host, peerID peer.ID, protoID protocol.ID) (*Client, error) {
	// If no host was given, create one.
	if p2pHost == nil {
		var err error
		p2pHost, err = libp2p.New(context.Background())
		if err != nil {
			return nil, err
		}
	}

	// Start a client
	return &Client{
		ctxLock: newCtxMutex(),
		host:    p2pHost,
		peerID:  peerID,
		protoID: protoID,
	}, nil
}

// Connect connects the client to the host at the location specified by
// hostname.  The value of hostname is a host or host:port, where the host is a
// hostname or IP address.
func (c *Client) Connect(ctx context.Context, hostname string) error {
	port := defaultLibp2pPort
	var netProto string
	if hostname == "" {
		hostname = "127.0.0.1"
		netProto = "ip4"
	} else {
		hostport := strings.SplitN(hostname, ":", 2)
		if len(hostport) > 1 {
			hostname = hostport[0]
			var err error
			port, err = strconv.Atoi(hostport[1])
			if err != nil {
				return err
			}
		}

		// Determine if hostname is a host name or IP address.
		ip := net.ParseIP(hostname)
		if ip == nil {
			netProto = "dns"
		} else if ip.To4() != nil {
			netProto = "ip4"
		} else if ip.To16() != nil {
			netProto = "ip6"
		} else {
			return fmt.Errorf("host %q does not appear to be a hostname or ip address", hostname)
		}
	}

	maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%d", netProto, hostname, port))
	if err != nil {
		return err
	}

	return c.ConnectAddrs(ctx, maddr)
}

func (c *Client) ConnectAddrs(ctx context.Context, maddrs ...multiaddr.Multiaddr) error {
	addrInfo := peer.AddrInfo{
		ID:    c.peerID,
		Addrs: maddrs,
	}

	return c.host.Connect(ctx, addrInfo)
}

// Self return the peer ID of this client.
func (c *Client) Self() peer.ID {
	return c.host.ID()
}

// Close resets and closes the network stream if one exists,
func (c *Client) Close() error {
	err := c.ctxLock.Lock(context.Background())
	if err != nil {
		return err
	}
	defer c.ctxLock.Unlock()

	if c.stream != nil {
		c.closeStream()
	}

	return nil
}

// SendRequest sends out a request.
func (c *Client) SendRequest(ctx context.Context, msg proto.Message, decodeRsp DecodeResponseFunc) error {
	err := c.ctxLock.Lock(ctx)
	if err != nil {
		return err
	}
	defer c.ctxLock.Unlock()

	err = c.sendMessage(ctx, msg)
	if err != nil {
		return fmt.Errorf("cannot sent request: %w", err)
	}

	if err = c.ctxReadMsg(ctx, decodeRsp); err != nil {
		c.closeStream()

		return fmt.Errorf("cannot read response: %w", err)
	}

	return nil
}

// SendMessage sends out a message
func (c *Client) SendMessage(ctx context.Context, msg proto.Message) error {
	err := c.ctxLock.Lock(ctx)
	if err != nil {
		return err
	}
	defer c.ctxLock.Unlock()

	return c.sendMessage(ctx, msg)
}

func (c *Client) sendMessage(ctx context.Context, msg proto.Message) error {
	err := c.prepStreamReader(ctx)
	if err != nil {
		return err
	}

	if err = writeMsg(c.stream, msg); err != nil {
		c.closeStream()
		return err
	}

	return nil
}

func (c *Client) prepStreamReader(ctx context.Context) error {
	if c.stream == nil {
		nstr, err := c.host.NewStream(ctx, c.peerID, c.protoID)
		if err != nil {
			return err
		}

		c.r = msgio.NewVarintReaderSize(nstr, network.MessageSizeMax)
		c.stream = nstr
	}

	return nil
}

func (c *Client) closeStream() {
	_ = c.stream.Reset()
	c.stream = nil
	c.r = nil
}

func (c *Client) ctxReadMsg(ctx context.Context, decodeRsp DecodeResponseFunc) error {
	done := make(chan struct{})
	var err error
	go func(r msgio.ReadCloser) {
		defer close(done)
		var data []byte
		data, err = r.ReadMsg()
		defer r.ReleaseMsg(data)
		if err != nil {
			return
		}
		err = decodeRsp(data)
	}(c.r)

	t := time.NewTimer(readMessageTimeout)
	defer t.Stop()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}

	return err
}
