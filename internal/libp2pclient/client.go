package libp2pclient

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/storetheindex/internal/p2putil"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
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

// Timeout to wait for a response after a request is sent
const readMessageTimeout = 10 * time.Second

// ErrReadTimeout is an error that occurs when no message is read within the
// timeout period
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// NewClient creates a new libp2pclient Client that connects to a specific peer
// and protocolID
func NewClient(h host.Host, peerID peer.ID, protoID protocol.ID, options ...ClientOption) (*Client, error) {
	var cfg clientConfig
	if err := cfg.apply(options...); err != nil {
		return nil, err
	}

	// Start a client
	return &Client{
		ctxLock: newCtxMutex(),
		host:    h,
		peerID:  peerID,
		protoID: protoID,
	}, nil
}

// Self return the peer ID of this client
func (c *Client) Self() peer.ID {
	return c.host.ID()
}

// Close resets and closes the network stream if one exists
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

// SendRequest sends out a request
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

	if err = p2putil.WriteMsg(c.stream, msg); err != nil {
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
