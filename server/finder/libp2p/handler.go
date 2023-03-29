package p2pfindserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-libipni/apierror"
	p2pclient "github.com/ipni/go-libipni/find/client/p2p"
	"github.com/ipni/go-libipni/find/model"
	pb "github.com/ipni/go-libipni/find/pb"
	"github.com/ipni/storetheindex/internal/counter"
	"github.com/ipni/storetheindex/internal/libp2pserver"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/server/finder/handler"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"google.golang.org/protobuf/proto"
)

var log = logging.Logger("indexer/find")

// handler handles requests for the providers resource
type libp2pHandler struct {
	findHandler *handler.FindHandler
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.FindMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, registry *registry.Registry, indexCounts *counter.IndexCounts) *libp2pHandler {
	return &libp2pHandler{
		findHandler: handler.NewFindHandler(indexer, registry, indexCounts),
	}
}

func (h *libp2pHandler) ProtocolID() protocol.ID {
	return p2pclient.FindProtocolID
}

func (h *libp2pHandler) HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte) (proto.Message, error) {
	var req pb.FindMessage
	err := proto.Unmarshal(msgbytes, &req)
	if err != nil {
		return nil, err
	}

	var rspType pb.FindMessage_MessageType
	var handle handlerFunc
	switch req.GetType() {
	case pb.FindMessage_FIND:
		handle = h.find
		rspType = pb.FindMessage_FIND_RESPONSE
	case pb.FindMessage_GET_PROVIDER:
		handle = h.getProvider
		rspType = pb.FindMessage_GET_PROVIDER_RESPONSE
	case pb.FindMessage_LIST_PROVIDERS:
		handle = h.listProviders
		rspType = pb.FindMessage_LIST_PROVIDERS_RESPONSE
	case pb.FindMessage_GET_STATS:
		handle = h.getStats
		rspType = pb.FindMessage_GET_STATS_RESPONSE
	default:
		return nil, fmt.Errorf("unsupported message type %d", req.GetType())
	}

	data, err := handle(ctx, msgPeer, &req)
	if err != nil {
		err = libp2pserver.HandleError(err, req.GetType().String())
		data = apierror.EncodeError(err)
		rspType = pb.FindMessage_ERROR_RESPONSE
	}

	return &pb.FindMessage{
		Type: rspType,
		Data: data,
	}, nil
}

func (h *libp2pHandler) RefreshStats() {
	h.findHandler.RefreshStats()
}

func (h *libp2pHandler) find(ctx context.Context, p peer.ID, msg *pb.FindMessage) ([]byte, error) {
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

	r, err := h.findHandler.Find(req.Multihashes)
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

func (h *libp2pHandler) listProviders(ctx context.Context, p peer.ID, msg *pb.FindMessage) ([]byte, error) {
	data, err := h.findHandler.ListProviders()
	if err != nil {
		log.Errorw("cannot list providers", "err", err)
		return nil, apierror.New(nil, http.StatusInternalServerError)
	}

	return data, nil
}

func (h *libp2pHandler) getProvider(ctx context.Context, p peer.ID, msg *pb.FindMessage) ([]byte, error) {
	var providerID peer.ID
	err := json.Unmarshal(msg.GetData(), &providerID)
	if err != nil {
		log.Errorw("error unmarshalling GetProvider request", "err", err)
		return nil, apierror.New(errors.New("cannot decode request"), http.StatusBadRequest)
	}

	data, err := h.findHandler.GetProvider(providerID)
	if err != nil {
		log.Errorw("cannot get provider", "err", err)
		return nil, apierror.New(nil, http.StatusInternalServerError)
	}

	if len(data) == 0 {
		return nil, apierror.New(errors.New("provider not found"), http.StatusNotFound)
	}

	return data, nil
}

func (h *libp2pHandler) getStats(ctx context.Context, p peer.ID, msg *pb.FindMessage) ([]byte, error) {
	data, err := h.findHandler.GetStats()
	if err != nil {
		log.Errorw("cannot get stats", "err", err)
		return nil, apierror.New(nil, http.StatusInternalServerError)
	}

	return data, nil
}
