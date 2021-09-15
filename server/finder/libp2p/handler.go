package p2pfinderserver

import (
	"context"
	"fmt"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/handler"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("finderp2pserver")

// handler handles requests for the providers resource
type libp2pHandler struct {
	finderHandler *handler.FinderHandler
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.FinderMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, registry *providers.Registry) *libp2pHandler {
	return &libp2pHandler{
		finderHandler: handler.NewFinderHandler(indexer, registry),
	}
}

func (h *libp2pHandler) ProtocolID() protocol.ID {
	return v0.FinderProtocolID
}

func (h *libp2pHandler) HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte) (proto.Message, error) {
	var req pb.FinderMessage
	err := req.Unmarshal(msgbytes)
	if err != nil {
		return nil, err
	}

	var rspType pb.FinderMessage_MessageType
	var handle handlerFunc
	switch req.GetType() {
	case pb.FinderMessage_GET:
		log.Debug("Handle new GET message")
		handle = h.get
		rspType = pb.FinderMessage_GET_RESPONSE
	default:
		msg := "ussupported message type"
		log.Errorw(msg, "type", req.GetType())
		return nil, fmt.Errorf("%s %d", msg, req.GetType())
	}

	data, err := handle(ctx, msgPeer, &req)
	if err != nil {
		err = libp2pserver.HandleError(err, req.GetType().String())
		data = v0.EncodeError(err)
		rspType = pb.FinderMessage_ERROR_RESPONSE
	}

	return &pb.FinderMessage{
		Type: rspType,
		Data: data,
	}, nil
}

func (h *libp2pHandler) get(ctx context.Context, p peer.ID, msg *pb.FinderMessage) ([]byte, error) {
	req, err := models.UnmarshalFindRequest(msg.GetData())
	if err != nil {
		return nil, err
	}

	r, err := h.finderHandler.MakeFindResponse(req.Multihashes)
	if err != nil {
		return nil, err
	}
	return models.MarshalFindResponse(r)
}
