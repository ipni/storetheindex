package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"

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

type Server struct {
	server    *http.Server
	listener  net.Listener
	healthMsg string
	indexer   indexer.Interface
	ingester  *ingest.Ingester
	registry  *registry.Registry
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
		server:   server,
		listener: l,
		indexer:  indexer,
		ingester: ingester,
		registry: registry,
	}

	s.healthMsg = "ready"
	if opts.version != "" {
		s.healthMsg += " " + opts.version
	}

	mux.HandleFunc("/announce", s.putAnnounce)
	mux.HandleFunc("/health", s.getHealth)
	mux.HandleFunc("/register", s.postRegisterProvider)

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
	return s.server.Shutdown(context.Background())
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
