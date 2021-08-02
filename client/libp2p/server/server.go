package p2pserver

import (
	"context"
	"io"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/client"
	p2pclient "github.com/filecoin-project/storetheindex/client/libp2p/client"
	"github.com/filecoin-project/storetheindex/client/libp2p/net"
	pb "github.com/filecoin-project/storetheindex/client/libp2p/pb"
	"github.com/filecoin-project/storetheindex/client/models"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
)

// Idle time before the stream is closed
var streamIdleTimeout = 1 * time.Minute
var _ client.Server = &Server{}

var log = logging.Logger("p2pserver")

// Server handles client requests over libp2p
type Server struct {
	ctx       context.Context
	host      host.Host
	self      peer.ID
	engine    *indexer.Engine
	protocols []protocol.ID
}

func (s *Server) setProtocolHandler(h network.StreamHandler) {
	// For every announced protocol set this new handler.
	for _, p := range s.protocols {
		s.host.SetStreamHandler(p, h)
	}
}

// Endpoint returns the endpoint of the protocol server.
func (s *Server) Endpoint() client.Endpoint {
	return p2pclient.P2pEndpoint(s.host.ID())
}

// New creates a new libp2p server
func New(ctx context.Context, h host.Host, e *indexer.Engine, options ...ServerOption) (*Server, error) {
	var cfg serverConfig
	if err := cfg.apply(append([]ServerOption{serverDefaults}, options...)...); err != nil {
		return nil, err
	}
	protocols := []protocol.ID{pid}

	s := &Server{
		ctx:       ctx,
		host:      h,
		self:      h.ID(),
		engine:    e,
		protocols: protocols,
	}

	s.setProtocolHandler(s.handleNewStream)

	return s, nil
}

// p2pserver handler signature
type handler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (s *Server) handlerForMsgType(t pb.Message_MessageType) handler {
	switch t {
	case pb.Message_GET:
		log.Debug("Handle new GET message")
		return s.handleGet
	}
	// NOTE: add here the processing of additional message types
	// that want to be supported over this protocol.

	return nil
}

func (s *Server) handleGet(ctx context.Context, p peer.ID, msg *pb.Message) (*pb.Message, error) {

	req, err := models.UnmarshalReq(msg.GetData())
	if err != nil {
		return nil, err
	}

	r, err := models.PopulateResp(s.engine, req.Cids)
	if err != nil {
		return nil, err
	}
	rb, err := models.MarshalResp(r)
	if err != nil {
		return nil, err
	}

	resp := &pb.Message{
		Type: pb.Message_RESPONSE,
		Data: rb,
	}

	return resp, nil
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
		err = net.WriteMsg(stream, resp)
		if err != nil {
			return false
		}

	}
}
