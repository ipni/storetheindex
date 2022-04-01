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
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer/admin")

type Server struct {
	cancel context.CancelFunc
	l      net.Listener
	server *http.Server
}

func New(listen string, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, options ...ServerOption) (*Server, error) {
	if ingester == nil {
		panic("ingester cannot be nil")
	}
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

	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		cancel: cancel,
		l:      l,
		server: server,
	}

	h := newHandler(ctx, indexer, ingester, reg)

	// Set protocol handlers
	// Import routes
	r.HandleFunc("/import/manifest/{provider}", h.importManifest).Methods(http.MethodPost)
	r.HandleFunc("/import/cidlist/{provider}", h.importCidList).Methods(http.MethodPost)

	// Admin routes
	r.HandleFunc("/healthcheck", h.healthCheckHandler).Methods(http.MethodGet)

	// Ingester routes
	r.HandleFunc("/ingest/allow/{peer}", h.allowPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/block/{peer}", h.blockPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/sync/{peer}", h.sync).Methods(http.MethodPost)
	r.HandleFunc("/ingest/reloadpolicy", h.reloadPolicy).Methods(http.MethodPost)

	// Metrics routes
	r.Handle("/metrics", metrics.Start(coremetrics.DefaultViews))
	r.PathPrefix("/debug/pprof").Handler(pprof.WithProfile())

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
	s.cancel() // stop any sync in progress
	return s.server.Shutdown(ctx)
}
