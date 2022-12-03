package p2pfinderserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/counter"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/server/finder/handler"
	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer/finder")

// handler handles requests for the providers resource
type libp2pHandler struct {
	finderHandler *handler.FinderHandler
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.FinderMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, registry *registry.Registry, indexCounts *counter.IndexCounts) *libp2pHandler {
	return &libp2pHandler{
		finderHandler: handler.NewFinderHandler(indexer, registry, indexCounts),
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
	case pb.FinderMessage_FIND:
		handle = h.find
		rspType = pb.FinderMessage_FIND_RESPONSE
	case pb.FinderMessage_GET_PROVIDER:
		handle = h.getProvider
		rspType = pb.FinderMessage_GET_PROVIDER_RESPONSE
	case pb.FinderMessage_LIST_PROVIDERS:
		handle = h.listProviders
		rspType = pb.FinderMessage_LIST_PROVIDERS_RESPONSE
	case pb.FinderMessage_GET_STATS:
		handle = h.getStats
		rspType = pb.FinderMessage_GET_STATS_RESPONSE
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

func (h *libp2pHandler) RefreshStats() {
	h.finderHandler.RefreshStats()
}

func (h *libp2pHandler) find(ctx context.Context, p peer.ID, msg *pb.FinderMessage) ([]byte, error) {
	startTime := time.Now()

	req, err := model.UnmarshalFindRequest(msg.GetData())
	if err != nil {
		return nil, err
	}

	var found bool
	defer func() {
		msecPerMh := coremetrics.MsecSince(startTime) / float64(len(req.Multihashes))
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(tag.Insert(metrics.Method, "libp2p"), tag.Insert(metrics.Found, fmt.Sprintf("%v", found))),
			stats.WithMeasurements(metrics.FindLatency.M(msecPerMh)))
	}()

	r, err := h.finderHandler.Find(req.Multihashes)
	if err != nil {
		return nil, err
	}
	data, err := model.MarshalFindResponse(r)
	if err != nil {
		return nil, err
	}

	if len(r.MultihashResults) > 0 {
		found = true
	}

	return data, nil
}

func (h *libp2pHandler) listProviders(ctx context.Context, p peer.ID, msg *pb.FinderMessage) ([]byte, error) {
	data, err := h.finderHandler.ListProviders()
	if err != nil {
		log.Errorw("cannot list providers", "err", err)
		return nil, v0.NewError(nil, http.StatusInternalServerError)
	}

	return data, nil
}

func (h *libp2pHandler) getProvider(ctx context.Context, p peer.ID, msg *pb.FinderMessage) ([]byte, error) {
	var providerID peer.ID
	err := json.Unmarshal(msg.GetData(), &providerID)
	if err != nil {
		log.Errorw("error unmarshalling GetProvider request", "err", err)
		return nil, v0.NewError(errors.New("cannot decode request"), http.StatusBadRequest)
	}

	data, err := h.finderHandler.GetProvider(providerID)
	if err != nil {
		log.Errorw("cannot get provider", "err", err)
		return nil, v0.NewError(nil, http.StatusInternalServerError)
	}

	if len(data) == 0 {
		return nil, v0.NewError(errors.New("provider not found"), http.StatusNotFound)
	}

	return data, nil
}

func (h *libp2pHandler) getStats(ctx context.Context, p peer.ID, msg *pb.FinderMessage) ([]byte, error) {
	data, err := h.finderHandler.GetStats()
	if err != nil {
		log.Errorw("cannot get stats", "err", err)
		return nil, v0.NewError(nil, http.StatusInternalServerError)
	}

	return data, nil
}
