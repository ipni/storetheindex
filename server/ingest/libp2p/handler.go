package p2pingestserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	v0 "github.com/ipni/storetheindex/api/v0"
	pb "github.com/ipni/storetheindex/api/v0/ingest/pb"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/libp2pserver"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/server/ingest/handler"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var log = logging.Logger("indexer/ingest")

// handler handles requests for the providers resource
type libp2pHandler struct {
	ingestHandler *handler.IngestHandler
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.IngestMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, ingester *ingest.Ingester, registry *registry.Registry) *libp2pHandler {
	return &libp2pHandler{
		ingestHandler: handler.NewIngestHandler(indexer, ingester, registry),
	}
}

func (h *libp2pHandler) ProtocolID() protocol.ID {
	return v0.IngestProtocolID
}

func (h *libp2pHandler) HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte) (proto.Message, error) {
	var req pb.IngestMessage
	err := req.Unmarshal(msgbytes)
	if err != nil {
		return nil, err
	}

	var handle handlerFunc
	var rspType pb.IngestMessage_MessageType
	switch req.GetType() {
	case pb.IngestMessage_DISCOVER_PROVIDER:
		handle = h.discoverProvider
		rspType = pb.IngestMessage_DISCOVER_PROVIDER_RESPONSE
	case pb.IngestMessage_REGISTER_PROVIDER:
		handle = h.registerProvider
		rspType = pb.IngestMessage_REGISTER_PROVIDER_RESPONSE
	case pb.IngestMessage_REMOVE_PROVIDER:
		handle = h.removeProvider
		rspType = pb.IngestMessage_REMOVE_PROVIDER_RESPONSE
	case pb.IngestMessage_INDEX_CONTENT:
		handle = h.indexContent
		rspType = pb.IngestMessage_INDEX_CONTENT_RESPONSE
	default:
		msg := "ussupported message type"
		log.Errorw(msg, "type", req.GetType())
		return nil, fmt.Errorf("%s %d", msg, req.GetType())
	}

	data, err := handle(ctx, msgPeer, &req)
	if err != nil {
		err = libp2pserver.HandleError(err, req.GetType().String())
		data = v0.EncodeError(err)
		rspType = pb.IngestMessage_ERROR_RESPONSE
	}

	return &pb.IngestMessage{
		Type: rspType,
		Data: data,
	}, nil
}

func (h *libp2pHandler) discoverProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	err := h.ingestHandler.DiscoverProvider(msg.GetData())
	return nil, err
}

func (h *libp2pHandler) registerProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	err := h.ingestHandler.RegisterProvider(ctx, msg.GetData())
	return nil, err
}

func (h *libp2pHandler) removeProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	return nil, v0.NewError(nil, http.StatusNotImplemented)
}

func (h *libp2pHandler) indexContent(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	err := h.ingestHandler.IndexContent(ctx, msg.GetData())
	return nil, err
}
