package p2pingestserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0"
	pb "github.com/filecoin-project/storetheindex/api/v0/ingest/pb"
	"github.com/filecoin-project/storetheindex/internal/handler"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("indexer/ingest")

// handler handles requests for the providers resource
type libp2pHandler struct {
	ingestHandler *handler.IngestHandler
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.IngestMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, registry *registry.Registry) *libp2pHandler {
	return &libp2pHandler{
		ingestHandler: handler.NewIngestHandler(indexer, registry),
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
		handle = h.DiscoverProvider
		rspType = pb.IngestMessage_DISCOVER_PROVIDER_RESPONSE
	case pb.IngestMessage_GET_PROVIDER:
		handle = h.GetProvider
		rspType = pb.IngestMessage_GET_PROVIDER_RESPONSE
	case pb.IngestMessage_LIST_PROVIDERS:
		handle = h.ListProviders
		rspType = pb.IngestMessage_LIST_PROVIDERS_RESPONSE
	case pb.IngestMessage_REGISTER_PROVIDER:
		handle = h.RegisterProvider
		rspType = pb.IngestMessage_REGISTER_PROVIDER_RESPONSE
	case pb.IngestMessage_REMOVE_PROVIDER:
		handle = h.RemoveProvider
		rspType = pb.IngestMessage_REMOVE_PROVIDER_RESPONSE
	case pb.IngestMessage_INDEX_CONTENT:
		handle = h.IndexContent
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

func (h *libp2pHandler) DiscoverProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	err := h.ingestHandler.DiscoverProvider(msg.GetData())
	return nil, err
}

func (h *libp2pHandler) ListProviders(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	data, err := h.ingestHandler.ListProviders()
	if err != nil {
		log.Errorw("cannot list providers", "err", err)
		return nil, v0.NewError(nil, http.StatusInternalServerError)
	}

	return data, nil
}

func (h *libp2pHandler) GetProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	var providerID peer.ID
	err := json.Unmarshal(msg.GetData(), &providerID)
	if err != nil {
		log.Errorw("error unmarshalling GetProvider request", "err", err)
		return nil, v0.NewError(errors.New("cannot decode request"), http.StatusBadRequest)
	}

	data, err := h.ingestHandler.GetProvider(providerID)
	if err != nil {
		log.Errorw("cannot get provider", "err", err)
		return nil, v0.NewError(nil, http.StatusInternalServerError)
	}

	if len(data) == 0 {
		return nil, v0.NewError(errors.New("provider not found"), http.StatusNotFound)
	}

	return data, nil
}

func (h *libp2pHandler) RegisterProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	err := h.ingestHandler.RegisterProvider(msg.GetData())
	return nil, err
}

func (h *libp2pHandler) RemoveProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	return nil, v0.NewError(nil, http.StatusNotImplemented)
}

func (h *libp2pHandler) IndexContent(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	err := h.ingestHandler.IndexContent(msg.GetData())
	return nil, err
}
