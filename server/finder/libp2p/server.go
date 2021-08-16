package p2pfinderserver

import (
	"context"
	"io"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/finder"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/server/finder/libp2p/handler"
	"github.com/filecoin-project/storetheindex/server/net"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
)

// Idle time before the stream is closed
var streamIdleTimeout = 1 * time.Minute
var _ finder.Server = &Server{}

var log = logging.Logger("p2pserver")

// Server handles client requests over libp2p
type Server struct {
	ctx           context.Context
	finderHandler *handler.Finder
	host          host.Host
	self          peer.ID
}

// Endpoint returns the endpoint of the protocol server.
func (s *Server) Endpoint() net.Endpoint {
	return net.P2PEndpoint(s.host.ID())
}

// New creates a new libp2p server
func New(ctx context.Context, h host.Host, engine *indexer.Engine, registry *providers.Registry, options ...ServerOption) (*Server, error) {
	var cfg serverConfig
	if err := cfg.apply(append([]ServerOption{serverDefaults}, options...)...); err != nil {
		return nil, err
	}

	s := &Server{
		ctx:           ctx,
		finderHandler: handler.NewFinder(engine, registry),
		host:          h,
		self:          h.ID(),
	}

	// Set handler for each announced protocol
	h.SetStreamHandler(pid0, s.handleNewStream)

	return s, nil
}

func (s *Server) handlerForMsgType(t pb.Message_MessageType) handler.HandlerFunc {
	switch t {
	case pb.Message_GET:
		log.Debug("Handle new GET message")
		return s.finderHandler.Get
	}
	// NOTE: add here the processing of additional message types
	// that want to be supported over this protocol.

	return nil
}

// handleNewStream implements the network.StreamHandler
func (s *Server) handleNewStream(stream network.Stream) {
	if s.handleNewMessages(stream) {
		// If we exited without error, close gracefully.
		_ = stream.Close()
	} else {
		// otherwise, send an error.
		_ = stream.Reset()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream conveniently).
func (s *Server) handleNewMessages(stream network.Stream) bool {
	ctx := s.ctx
	r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)

	mPeer := stream.Conn().RemotePeer()

	timer := time.AfterFunc(streamIdleTimeout, func() { _ = stream.Reset() })
	defer timer.Stop()

	for {
		var req pb.Message
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			return err == io.EOF
		}
		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			return false
		}

		timer.Reset(streamIdleTimeout)

		handler := s.handlerForMsgType(req.GetType())
		if handler == nil {
			return false
		}

		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			return false
		}

		if resp == nil {
			continue
		}

		// send out response msg
		err = net.WriteFinderMsg(stream, resp)
		if err != nil {
			return false
		}

	}
}
