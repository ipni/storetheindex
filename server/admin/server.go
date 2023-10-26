package admin

import (
	"context"
	"fmt"
	"net"
	"net/http"

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
	handler  *adminHandler
	listener net.Listener
	server   *http.Server
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, id peer.ID, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, reloadErrChan chan<- chan error, options ...Option) (*Server, error) {
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

	ctx, cancel := context.WithCancel(context.Background())
	h := newHandler(ctx, id, indexer, ingester, reg, reloadErrChan)

	s := &Server{
		cancel:   cancel,
		handler:  h,
		listener: l,
		server:   server,
	}

	// Admin routes
	mux.HandleFunc("/freeze", h.freeze)
	mux.HandleFunc("/status", h.status)
	mux.HandleFunc("/healthcheck", h.healthCheckHandler)
	mux.HandleFunc("/importproviders", h.importProviders)
	mux.HandleFunc("/reloadconfig", h.reloadConfig)

	// Ingester routes
	mux.HandleFunc("/ingest/allow/", h.allowPeer)
	mux.HandleFunc("/ingest/block/", h.blockPeer)
	mux.HandleFunc("/ingest/sync/", h.sync)

	// Assignment routes
	mux.HandleFunc("/ingest/assign/", h.assignPeer)
	mux.HandleFunc("/ingest/assigned", h.listAssignedPeers)
	mux.HandleFunc("/ingest/handoff/", h.handoffPeer)
	mux.HandleFunc("/ingest/unassign/", h.unassignPeer)
	mux.HandleFunc("/ingest/preferred", h.listPreferredPeers)

	// Metrics routes
	mux.Handle("/metrics/", metrics.Start(append(coremetrics.DefaultViews, coremetrics.PebbleViews...)))
	mux.Handle("/debug/pprof/", pprof.WithProfile())

	// Telemetry routes
	mux.HandleFunc("/telemetry/providers", h.listTelemetry)
	mux.HandleFunc("/telemetry/providers/", h.getTelemetry)

	// Config routes
	mux.HandleFunc("/config/log/level", setLogLevel)
	mux.HandleFunc("/config/log/subsystems", listLogSubSystems)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("admin http server shutdown")
	s.cancel() // stop any sync in progress
	s.handler.pendingSyncs.Wait()
	return s.server.Shutdown(context.Background())
}
