package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/storetheindex/ingest"
	"github.com/ipni/storetheindex/internal/httpserver"
	"github.com/ipni/storetheindex/registry"
)

var log = logging.Logger("indexer/ingest")

type Server struct {
	server        *http.Server
	listener      net.Listener
	ingestHandler handler
	healthMsg     string
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
		ingestHandler: handler{
			indexer:  indexer,
			ingester: ingester,
			registry: registry,
		},
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

	if r.Header.Get("Content-Type") == "application/json" {
		err = json.NewDecoder(r.Body).Decode(&an)
	} else {
		err = an.UnmarshalCBOR(r.Body)
	}
	if err != nil {
		httpserver.HandleError(w, err, "announce")
		return
	}

	if err = s.ingestHandler.announce(an); err != nil {
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = s.ingestHandler.registerProvider(r.Context(), body)
	if err != nil {
		httpserver.HandleError(w, err, "register")
		return
	}

	w.WriteHeader(http.StatusOK)
}
