package adminserver

import (
	"context"
	"net"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("adminserver")

type Server struct {
	server *http.Server
	l      net.Listener
}

func New(ctx context.Context, listen string, indexer indexer.Interface, ingester ingest.Ingester, options ...ServerOption) (*Server, error) {
	var cfg serverConfig
	if err := cfg.apply(append([]ServerOption{serverDefaults}, options...)...); err != nil {
		return nil, err
	}
	var err error

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)
	server := &http.Server{
		Handler:      r,
		WriteTimeout: cfg.apiWriteTimeout,
		ReadTimeout:  cfg.apiReadTimeout,
	}
	s := &Server{server, l}

	h := newHandler(ctx, indexer, ingester)

	// Set protocol handlers
	// Import routes
	r.HandleFunc("/import/manifest/{minerid}", h.importManifest).Methods("POST")
	r.HandleFunc("/import/cidlist/{minerid}", h.importCidList).Methods("POST")

	// Admin routes
	r.HandleFunc("/healthcheck", h.healthCheckHandler).Methods("GET")

	// Ingester routes
	r.HandleFunc("/ingest/subscribe/{provider}", h.subscribe).Methods("GET")
	r.HandleFunc("/ingest/unsubscribe/{provider}", h.unsubscribe).Methods("GET")
	r.HandleFunc("/ingest/sync/{provider}", h.sync).Methods("GET")

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("api listening", "addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
