package adminserver

import (
	"context"
	"net"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	ingestion "github.com/filecoin-project/storetheindex/api/v0/ingest"
	"github.com/filecoin-project/storetheindex/server/admin/http/handler"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("adminserver")

type Server struct {
	server *http.Server
	l      net.Listener
}

func New(listen string, indexer indexer.Interface, ingester ingestion.Ingester, options ...ServerOption) (*Server, error) {
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

	adminHandler := handler.New(indexer, ingester)

	// Set protocol handlers
	// Import routes
	r.HandleFunc("/import/manifest/{minerid}", adminHandler.ImportManifest).Methods("POST")
	r.HandleFunc("/import/cidlist/{minerid}", adminHandler.ImportCidList).Methods("POST")

	// Admin routes
	r.HandleFunc("/healthcheck", adminHandler.HealthCheckHandler).Methods("GET")

	// Ingester routes
	r.HandleFunc("/ingest/subscribe/{minerid}", adminHandler.Subscribe).Methods("POST")
	r.HandleFunc("/ingest/unsubscribe/{minerid}", adminHandler.Unsubscribe).Methods("POST")
	r.HandleFunc("/ingest/sync/{minerid}", adminHandler.Sync).Methods("POST")

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("api listening", "addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
