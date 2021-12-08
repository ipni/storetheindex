package adminserver

import (
	"context"
	"net"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/metrics/pprof"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer/admin")

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
	r.HandleFunc("/import/manifest/{provider}", h.importManifest).Methods(http.MethodPost)
	r.HandleFunc("/import/cidlist/{provider}", h.importCidList).Methods(http.MethodPost)

	// Admin routes
	r.HandleFunc("/healthcheck", h.healthCheckHandler).Methods(http.MethodGet)

	// Ingester routes
	r.HandleFunc("/ingest/subscribe/{provider}", h.subscribe).Methods(http.MethodGet)
	r.HandleFunc("/ingest/unsubscribe/{provider}", h.unsubscribe).Methods(http.MethodGet)
	r.HandleFunc("/ingest/sync/{provider}", h.sync).Methods(http.MethodGet)

	// Metrics routes
	r.Handle("/metrics", metrics.Start(coremetrics.DefaultViews))
	r.Handle("/debug/pprof", pprof.WithProfile())

	//Config routes
	registerSetLogLevelHandler(r)
	registerListLogSubSystems(r)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "listen_addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("admin http server shutdown")
	return s.server.Shutdown(ctx)
}
