package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/ingest/model"
	"github.com/ipni/storetheindex/internal/httpserver"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("indexer/ingest")

// maxBodySize is the limit on the request body size that the server will read.
// No request body should be this large, so any reuest exceeding this size is
// clearly in error.
const maxBodySize = 1024 * 1024

// maxAdStatusBatch is the maximum number of advertisement CIDs allowed in a
// single batch ad-status request.
const maxAdStatusBatch = 128

type Server struct {
	server          *http.Server
	listener        net.Listener
	healthMsg       string
	indexer         indexer.Interface
	ingester        *ingest.Ingester
	registry        *registry.Registry
	shutdownTimeout time.Duration
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, indexer indexer.Interface, ingester *ingest.Ingester, registry *registry.Registry, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	server := &http.Server{
		Handler:      mux,
		WriteTimeout: opts.writeTimeout,
		ReadTimeout:  opts.readTimeout,
	}
	s := &Server{
		server:          server,
		listener:        l,
		indexer:         indexer,
		ingester:        ingester,
		registry:        registry,
		shutdownTimeout: opts.shutdownTimeout,
	}

	s.healthMsg = "ready"
	if opts.version != "" {
		s.healthMsg += " " + opts.version
	}

	mux.HandleFunc("/announce", s.putAnnounce)
	mux.HandleFunc("/health", s.getHealth)
	mux.HandleFunc("/register", s.postRegisterProvider)
	mux.HandleFunc("/sync/status", s.listSyncStatus)
	mux.HandleFunc("/sync/status/", s.getSyncStatus)
	mux.HandleFunc("/sync/status/ad", s.batchAdStatus)
	mux.HandleFunc("/sync/status/ad/", s.getAdStatus)

	// Depricated
	mux.HandleFunc("/ingest/announce", s.putAnnounce)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("ingest http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("ingest http server shutdown")

	ctx := context.Background()
	if s.shutdownTimeout > 0 {
		tctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
		ctx = tctx
	}

	return s.server.Shutdown(ctx)
}

func (s *Server) putAnnounce(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPut) {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	defer r.Body.Close()

	var an message.Message
	var err error

	bodyReader := http.MaxBytesReader(w, r.Body, maxBodySize)
	if r.Header.Get("Content-Type") == "application/json" {
		err = json.NewDecoder(bodyReader).Decode(&an)
	} else {
		err = an.UnmarshalCBOR(bodyReader)
	}
	if err != nil {
		httpserver.HandleError(w, err, "announce")
		return
	}

	if err = s.announce(an); err != nil {
		httpserver.HandleError(w, err, "announce")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getHealth(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	w.Header().Set("Cache-Control", "no-cache")
	http.Error(w, s.healthMsg, http.StatusOK)
}

func (s *Server) listSyncStatus(w http.ResponseWriter, r *http.Request) {
	httpserver.EnableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}
	if _, ok := httpserver.AcceptsMediaType(w, r, false, httpserver.MediaTypeJson, httpserver.MediaTypeAny); !ok {
		return
	}

	if s.ingester == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	statuses := s.ingester.AllSyncStatuses()
	if len(statuses) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	out := make(map[string]any, len(statuses))
	for pubID, st := range statuses {
		out[pubID.String()] = st
	}

	data, err := json.Marshal(out)
	if err != nil {
		log.Errorw("cannot marshal sync statuses", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (s *Server) getSyncStatus(w http.ResponseWriter, r *http.Request) {
	httpserver.EnableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}
	if _, ok := httpserver.AcceptsMediaType(w, r, false, httpserver.MediaTypeJson, httpserver.MediaTypeAny); !ok {
		return
	}

	pubID, err := peer.Decode(path.Base(r.URL.Path))
	if err != nil {
		msg := "Cannot decode peer id"
		log.Errorw(msg, "id", path.Base(r.URL.Path), "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if s.ingester == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	st := s.ingester.SyncStatus(pubID)
	if st == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	data, err := json.Marshal(st)
	if err != nil {
		log.Errorw("cannot marshal sync status", "err", err, "publisher", pubID)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

type adStatusResponse struct {
	Ad         string `json:"Ad"`
	Indexed    bool   `json:"Indexed"`
	State      string `json:"State,omitempty"`
	SkipReason string `json:"SkipReason,omitempty"`
	Frozen     bool   `json:"Frozen"`
	Error      string `json:"Error,omitempty"`
}

func newAdStatusResponse(adCid cid.Cid, adState ingest.AdState) adStatusResponse {
	resp := adStatusResponse{
		Ad:      adCid.String(),
		Indexed: adState.Indexed(),
		Frozen:  adState.Frozen,
	}

	switch {
	case !adState.Known:
		resp.State = "unknown"
	case adState.Resync:
		resp.State = "resyncing"
	case adState.Processed && adState.Skipped:
		resp.State = "skipped"
		resp.SkipReason = adState.SkipReason
	case adState.Processed && !adState.Skipped:
		resp.State = "indexed"
	default:
		resp.State = "pending"
	}

	return resp
}

func (s *Server) getAdStatus(w http.ResponseWriter, r *http.Request) {
	httpserver.EnableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}
	if _, ok := httpserver.AcceptsMediaType(w, r, false, httpserver.MediaTypeJson, httpserver.MediaTypeAny); !ok {
		return
	}

	cidStr := path.Base(r.URL.Path)
	adCid, err := cid.Decode(cidStr)
	if err != nil {
		msg := "Cannot decode advertisement cid"
		log.Errorw(msg, "cid", cidStr, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if adCid.Prefix().Codec != cid.DagJSON && adCid.Prefix().Codec != cid.DagCBOR {
		msg := fmt.Sprintf("this endpoint expects an advertisement CID (dag-json or dag-cbor codec), got %d; use /cid/%s for content lookups", adCid.Prefix().Codec, cidStr)
		log.Debugw(msg, "cid", cidStr)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if s.ingester == nil {
		http.Error(w, "ingester not available", http.StatusServiceUnavailable)
		return
	}

	adState, err := s.ingester.GetAdState(r.Context(), adCid)
	if err != nil {
		log.Errorw("Failed to read advertisement processed state", "adCid", adCid, "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	resp := newAdStatusResponse(adCid, adState)

	data, err := json.Marshal(resp)
	if err != nil {
		log.Errorw("cannot marshal advertisement status", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

type batchAdStatusRequest struct {
	Ads []string `json:"Ads"`
}

type batchAdStatusResponse struct {
	Statuses []adStatusResponse `json:"Statuses"`
}

func (s *Server) batchAdStatus(w http.ResponseWriter, r *http.Request) {
	httpserver.EnableCors(w)

	// Hand-rolled method check (not httpserver.MethodOK) so we can set Allow
	// header and give a message that directs single-CID callers to the GET endpoint.
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed; use POST for batch or GET /sync/status/ad/<cid> for single advertisement", http.StatusMethodNotAllowed)
		return
	}

	if _, ok := httpserver.AcceptsMediaType(w, r, false, httpserver.MediaTypeJson, httpserver.MediaTypeAny); !ok {
		return
	}

	bodyReader := http.MaxBytesReader(w, r.Body, maxBodySize)
	defer r.Body.Close()

	var req batchAdStatusRequest
	if err := json.NewDecoder(bodyReader).Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, fmt.Sprintf("request body exceeds %d byte limit", maxBodySize), http.StatusBadRequest)
			return
		}
		http.Error(w, "malformed request body", http.StatusBadRequest)
		return
	}

	if len(req.Ads) == 0 {
		http.Error(w, "no advertisement CIDs provided", http.StatusBadRequest)
		return
	}

	if len(req.Ads) > maxAdStatusBatch {
		msg := fmt.Sprintf("batch size %d exceeds limit of %d", len(req.Ads), maxAdStatusBatch)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	if s.ingester == nil {
		http.Error(w, "ingester not available", http.StatusServiceUnavailable)
		return
	}

	results := make([]adStatusResponse, len(req.Ads))
	type indexedCid struct {
		idx int
		cid cid.Cid
	}
	var validCids []indexedCid

	for i, raw := range req.Ads {
		adCid, err := cid.Decode(raw)
		if err != nil {
			log.Debugw("cannot decode advertisement cid in batch", "raw", raw, "err", err)
			results[i] = adStatusResponse{
				Ad:    raw,
				Error: "cannot decode advertisement cid",
			}
			continue
		}

		if adCid.Prefix().Codec != cid.DagJSON && adCid.Prefix().Codec != cid.DagCBOR {
			results[i] = adStatusResponse{
				Ad:    adCid.String(),
				Error: fmt.Sprintf("this endpoint expects an advertisement CID (dag-json or dag-cbor codec), got %d; use /cid/%s for content lookups", adCid.Prefix().Codec, adCid.String()),
			}
			continue
		}

		validCids = append(validCids, indexedCid{i, adCid})
	}

	if len(validCids) > 0 {
		cids := make([]cid.Cid, len(validCids))
		for i, v := range validCids {
			cids[i] = v.cid
		}

		adStates, err := s.ingester.GetAdStates(r.Context(), cids)
		if err != nil {
			log.Errorw("Failed to read advertisement states", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		for i, st := range adStates {
			results[validCids[i].idx] = newAdStatusResponse(validCids[i].cid, st)
		}
	}

	resp := batchAdStatusResponse{Statuses: results}
	data, err := json.Marshal(resp)
	if err != nil {
		log.Errorw("cannot marshal batch advertisement status", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (s *Server) postRegisterProvider(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	bodyReader := http.MaxBytesReader(w, r.Body, maxBodySize)
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = s.registerProvider(r.Context(), body)
	if err != nil {
		httpserver.HandleError(w, err, "register")
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) announce(an message.Message) error {
	if len(an.Addrs) == 0 {
		return fmt.Errorf("must specify location to fetch on direct announcments")
	}

	// todo: require auth?

	addrs, err := an.GetAddrs()
	if err != nil {
		return fmt.Errorf("could not decode addrs from announce message: %w", err)
	}

	ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
	if err != nil {
		return err
	}
	if len(ais) > 1 {
		return errors.New("peer id must be the same for all addresses")
	}
	addrInfo := ais[0]

	if !s.registry.Allowed(addrInfo.ID) {
		err = fmt.Errorf("announce requests not allowed from peer %s", addrInfo.ID)
		return apierror.New(err, http.StatusForbidden)
	}
	cur, err := s.ingester.GetLatestSync(addrInfo.ID)
	if err == nil {
		if cur.Equals(an.Cid) {
			return nil
		}
	}

	// Use background context because this will be an async process. We don't
	// want to attach the context to the request context that started this.
	return s.ingester.Announce(context.Background(), an.Cid, addrInfo)
}

func (s *Server) registerProvider(ctx context.Context, data []byte) error {
	peerRec, err := model.ReadRegisterRequest(data)
	if err != nil {
		return fmt.Errorf("cannot read register request: %s", err)
	}

	if len(peerRec.PeerID) == 0 {
		return errors.New("missing peer id")
	}

	if err = s.registry.CheckSequence(peerRec.PeerID, peerRec.Seq); err != nil {
		return err
	}

	provider := peer.AddrInfo{
		ID:    peerRec.PeerID,
		Addrs: peerRec.Addrs,
	}
	publisher := peer.AddrInfo{}

	return s.registry.Update(ctx, provider, publisher, cid.Undef, nil, 0)
}

// TODO: Uncomment when supporting puts directly to indexer.
/*
// indexContent handles an IngestRequest
//
// Returning error is the same as return apierror.New(err, http.StatusBadRequest)
func (s *Server) indexContent(ctx context.Context, data []byte) error {
	ingReq, err := model.ReadIngestRequest(data)
	if err != nil {
		return fmt.Errorf("cannot read ingest request: %s", err)
	}

	if len(ingReq.ContextID) > schema.MaxContextIDLen {
		return errors.New("context id too long")
	}

	if len(ingReq.Metadata) > schema.MaxMetadataLen {
		return errors.New("metadata too long")
	}

	if err = s.registry.CheckSequence(ingReq.ProviderID, ingReq.Seq); err != nil {
		return err
	}

	maddrs, err := stringsToMultiaddrs(ingReq.Addrs)
	if err != nil {
		return err
	}

	provider := peer.AddrInfo{
		ID:    ingReq.ProviderID,
		Addrs: maddrs,
	}

	// Register provider if not registered, or update addreses if already registered
	err = s.registry.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	if err != nil {
		return err
	}

	value := indexer.Value{
		ProviderID:    ingReq.ProviderID,
		ContextID:     ingReq.ContextID,
		MetadataBytes: ingReq.Metadata,
	}
	err = s.indexer.Put(value, ingReq.Multihash)
	if err != nil {
		err = fmt.Errorf("cannot index content: %s", err)
		return apierror.New(err, http.StatusInternalServerError)
	}

	// TODO: update last update time for provider

	return nil
}

func stringsToMultiaddrs(addrs []string) ([]multiaddr.Multiaddr, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("bad address: %s", err)
		}
	}
	return maddrs, nil
}
*/
