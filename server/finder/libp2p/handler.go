package p2pfinderserver

import (
	"context"
	"fmt"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/handler"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// handler handles requests for the providers resource
type libp2pHandler struct {
	finderHandler *handler.FinderHandler
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.FinderMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, registry *registry.Registry) *libp2pHandler {
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
		handle = h.get
		rspType = pb.FinderMessage_GET_RESPONSE
	default:
		return nil, fmt.Errorf("unsupported message type %d", req.GetType())
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
	startTime := time.Now()

	req, err := model.UnmarshalFindRequest(msg.GetData())
	if err != nil {
		return nil, err
	}

	r, err := h.finderHandler.MakeFindResponse(req.Multihashes)
	if err != nil {
		return nil, err
	}
	data, err := model.MarshalFindResponse(r)
	if err != nil {
		return nil, err
	}

	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "libp2p")),
		stats.WithMeasurements(metrics.FindLatency.M(coremetrics.MsecSince(startTime))))

	return data, nil
}
