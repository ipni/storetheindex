package p2pingestserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/ingest/pb"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/gogo/protobuf/proto"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("ingestp2pserver")

// handler handles requests for the providers resource
type handler struct {
	indexer  indexer.Interface
	registry *providers.Registry
}

// handlerFunc is the function signature required by handlers in this package
type handlerFunc func(context.Context, peer.ID, *pb.IngestMessage) ([]byte, error)

func newHandler(indexer indexer.Interface, registry *providers.Registry) *handler {
	return &handler{
		indexer:  indexer,
		registry: registry,
	}
}

func (h *handler) ProtocolID() protocol.ID {
	return v0.IngestProtocolID
}

func (h *handler) HandleMessage(ctx context.Context, msgPeer peer.ID, msgbytes []byte, freeMsg func()) (proto.Message, error) {
	var req pb.IngestMessage
	err := req.Unmarshal(msgbytes)
	freeMsg()
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
	default:
		msg := "ussupported message type"
		log.Errorw(msg, "type", req.GetType())
		return nil, fmt.Errorf("%s %d", msg, req.GetType())
	}

	data, err := handle(ctx, msgPeer, &req)
	if err != nil {
		rspType = pb.IngestMessage_ERROR_RESPONSE
		data = v0.EncodeError(err)
	}

	return &pb.IngestMessage{
		Type: rspType,
		Data: data,
	}, nil
}

func (h *handler) DiscoverProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	var discoReq models.DiscoverRequest
	if err := json.Unmarshal(msg.GetData(), &discoReq); err != nil {
		log.Errorw("error unmarshalling discovery request", "err", err)
		return nil, v0.MakeError(http.StatusBadRequest, nil)
	}

	err := discoReq.VerifySignature()
	if err != nil {
		log.Errorw("signature not verified", "err", err, "provider", discoReq.ProviderID, "discover_addr", discoReq.DiscoveryAddr)
		return nil, v0.MakeError(http.StatusBadRequest, err)
	}

	err = h.registry.Discover(discoReq.ProviderID, discoReq.DiscoveryAddr, false)
	if err != nil {
		log.Errorw("cannot process discovery request", "err", err)
		return nil, v0.MakeError(http.StatusBadRequest, nil)
	}

	return nil, nil
}

func (h *handler) ListProviders(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	infos := h.registry.AllProviderInfo()

	responses := make([]models.ProviderInfo, len(infos))
	for i := range infos {
		providerInfoToApi(infos[i], &responses[i])
	}

	rb, err := json.Marshal(responses)
	if err != nil {
		log.Errorw("cannot marshal response", "err", err)
		return nil, v0.MakeError(http.StatusInternalServerError, nil)
	}

	return rb, nil
}

func (h *handler) GetProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	var providerID peer.ID
	err := json.Unmarshal(msg.GetData(), &providerID)
	if err != nil {
		log.Errorw("error unmarshalling GetProvider request", "err", err)
		return nil, v0.MakeError(http.StatusBadRequest, errors.New("cannot decode request"))
	}

	info := h.registry.ProviderInfo(providerID)
	if info == nil {
		return nil, v0.MakeError(http.StatusNotFound, nil)
	}

	var rsp models.ProviderInfo
	providerInfoToApi(info, &rsp)

	rb, err := json.Marshal(&rsp)
	if err != nil {
		log.Errorw("failed marshalling response", "err", err)
		return nil, v0.MakeError(http.StatusInternalServerError, nil)
	}

	return rb, nil
}

func (h *handler) RegisterProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	var regReq models.RegisterRequest
	err := json.Unmarshal(msg.GetData(), &regReq)
	if err != nil {
		log.Errorw("error unmarshalling registration request", "err", err)
		return nil, v0.MakeError(http.StatusBadRequest, errors.New("cannot decode request"))
	}

	err = regReq.VerifySignature()
	if err != nil {
		log.Errorw("signature not verified", "err", err, "provider", regReq.AddrInfo.ID)
		return nil, v0.MakeError(http.StatusBadRequest, err)
	}

	info := &providers.ProviderInfo{
		AddrInfo: regReq.AddrInfo,
	}
	err = h.registry.Register(info)
	if err != nil {
		log.Errorw("cannot process registration request", "err", err)
		return nil, v0.MakeError(http.StatusBadRequest, nil)
	}

	return nil, nil
}

func (h *handler) RemoveProvider(ctx context.Context, p peer.ID, msg *pb.IngestMessage) ([]byte, error) {
	return nil, v0.MakeError(http.StatusNotImplemented, nil)
}

func providerInfoToApi(pinfo *providers.ProviderInfo, apiModel *models.ProviderInfo) {
	*apiModel = models.MakeProviderInfo(pinfo.AddrInfo, pinfo.LastIndex, pinfo.LastIndexTime)
}
