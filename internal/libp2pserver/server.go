package libp2pserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/api/v0"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

// Idle time before the stream is closed
const streamIdleTimeout = time.Minute

var log = logging.Logger("indexer/libp2p")

type Handler interface {
	HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte) (proto.Message, error)
	ProtocolID() protocol.ID
}

// Server handles client requests over libp2p
type Server struct {
	ctx     context.Context
	handler Handler
	selfID  peer.ID
}

// ID returns the peer.ID of the protocol server.
func (s *Server) ID() peer.ID {
	return s.selfID
}

// New creates a new libp2p Server
func New(ctx context.Context, h host.Host, messageHandler Handler) *Server {
	s := &Server{
		ctx:     ctx,
		handler: messageHandler,
		selfID:  h.ID(),
	}

	// Set handler for each announced protocol
	h.SetStreamHandler(messageHandler.ProtocolID(), s.handleNewStream)

	return s
}

func HandleError(err error, reqType string) *v0.Error {
	var apierr *v0.Error
	if errors.As(err, &apierr) {
		if apierr.Status() >= 500 {
			log.Errorw(fmt.Sprint("cannot handle", reqType, "request"), "err", apierr.Error(), "status", apierr.Status())
			// Log the error and return only the 5xx status.
			return v0.NewError(nil, apierr.Status())
		}
	} else {
		apierr = v0.NewError(err, http.StatusBadRequest)
	}
	log.Infow(fmt.Sprint("bad", reqType, "request"), "err", apierr.Error(), "status", apierr.Status())
	return apierr
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
	handler := s.handler
	r := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)

	mPeer := stream.Conn().RemotePeer()

	timer := time.AfterFunc(streamIdleTimeout, func() { _ = stream.Reset() })
	defer timer.Stop()

	for {
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			return err == io.EOF
		}
		timer.Reset(streamIdleTimeout)

		resp, err := handler.HandleMessage(ctx, mPeer, msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			return true
		}

		// send out response msg
		err = writeMsg(stream, resp)
		if err != nil {
			return false
		}
	}
}
