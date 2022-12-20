package adminserver

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/metrics/pprof"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("indexer/admin")

type Server struct {
	cancel   context.CancelFunc
	listener net.Listener
	server   *http.Server
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, id peer.ID, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, reloadErrChan chan<- chan error, options ...ServerOption) (*Server, error) {
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
		cancel:   cancel,
		listener: l,
		server:   server,
	}

	h := newHandler(ctx, id, indexer, ingester, reg, reloadErrChan)

	// Set protocol handlers

	// Import routes
	r.HandleFunc("/import/manifest/{provider}", h.importManifest).Methods(http.MethodPost)
	r.HandleFunc("/import/cidlist/{provider}", h.importCidList).Methods(http.MethodPost)

	// Admin routes
	r.HandleFunc("/freeze", h.freeze).Methods(http.MethodPut)
	r.HandleFunc("/status", h.status).Methods(http.MethodGet)
	r.HandleFunc("/healthcheck", h.healthCheckHandler).Methods(http.MethodGet)
	r.HandleFunc("/importproviders", h.importProviders).Methods(http.MethodPost)
	r.HandleFunc("/reloadconfig", h.reloadConfig).Methods(http.MethodPost)

	// Ingester routes
	r.HandleFunc("/ingest/allow/{peer}", h.allowPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/block/{peer}", h.blockPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/sync/{peer}", h.sync).Methods(http.MethodPost)

	// Assignment routes
	r.HandleFunc("/ingest/assigned", h.listAssignedPeers).Methods(http.MethodGet)
	r.HandleFunc("/ingest/assign/{peer}", h.assignPeer).Methods(http.MethodPost)
	r.HandleFunc("/ingest/handoff/{peer}", h.handoffPeer).Methods(http.MethodPost)
	r.HandleFunc("/ingest/unassign/{peer}", h.unassignPeer).Methods(http.MethodPut)
	r.HandleFunc("/ingest/preferred", h.listPreferredPeers).Methods(http.MethodGet)

	// Metrics routes
	r.Handle("/metrics", metrics.Start(coremetrics.DefaultViews))
	r.PathPrefix("/debug/pprof").Handler(pprof.WithProfile())

	// Config routes
	registerSetLogLevelHandler(r)
	registerListLogSubSystems(r)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("admin http server shutdown")
	s.cancel() // stop any sync in progress
	return s.server.Shutdown(context.Background())
}
