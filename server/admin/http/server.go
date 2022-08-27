package adminserver

import (
	"context"
	"errors"
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
	cancel  context.CancelFunc
	l       net.Listener
	server  *http.Server
	handler *adminHandler
}

func New(listen string, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, reloadErrChan chan<- chan error, options ...ServerOption) (*Server, error) {
	if ingester == nil {
		return nil, errors.New("ingester cannot be nil")
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
		cancel:  cancel,
		l:       l,
		server:  server,
		handler: newHandler(ctx, indexer, ingester, reg, reloadErrChan),
	}

	// Set protocol handlers
	// Import routes
	r.HandleFunc("/import/manifest/{provider}", s.importManifest).Methods(http.MethodPost)
	r.HandleFunc("/import/cidlist/{provider}", s.importCidList).Methods(http.MethodPost)

	// Admin routes
	r.HandleFunc("/healthcheck", s.healthCheckHandler).Methods(http.MethodGet)
	r.HandleFunc("/importproviders", s.importProviders).Methods(http.MethodPost)
	r.HandleFunc("/reloadconfig", s.reloadConfig).Methods(http.MethodPost)

	// Ingester routes
	r.HandleFunc("/ingest/allow/{peer}", s.allowPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/block/{peer}", s.blockPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/sync/{peer}", s.sync).Methods(http.MethodPost)

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

func (s *Server) Close() error {
	log.Info("admin http server shutdown")
	s.cancel() // stop any sync in progress
	return s.server.Shutdown(context.Background())
}

func (s *Server) importManifest(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set", http.StatusInternalServerError)
		return
	}
	s.handler.importManifest(w, r)
}

func (s *Server) importCidList(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set", http.StatusInternalServerError)
		return
	}
	s.handler.importCidList(w, r)
}

func (s *Server) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set. not ready yet.", http.StatusInternalServerError)
		return
	}
	s.handler.healthCheckHandler(w, r)
}

func (s *Server) importProviders(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set. not ready yet.", http.StatusInternalServerError)
		return
	}
	s.handler.importProviders(w, r)
}

func (s *Server) reloadConfig(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set. not ready yet.", http.StatusInternalServerError)
		return
	}
	s.handler.reloadConfig(w, r)
}

func (s *Server) allowPeer(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set. not ready yet.", http.StatusInternalServerError)
		return
	}
	s.handler.allowPeer(w, r)
}

func (s *Server) blockPeer(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set. not ready yet.", http.StatusInternalServerError)
		return
	}
	s.handler.blockPeer(w, r)
}

func (s *Server) sync(w http.ResponseWriter, r *http.Request) {
	if s.handler == nil {
		http.Error(w, "handler not set. not ready yet.", http.StatusInternalServerError)
		return
	}
	s.handler.sync(w, r)
}
