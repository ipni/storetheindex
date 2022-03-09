package httpfinderserver

import (
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer/finder")

type Server struct {
	server *http.Server
	l      net.Listener
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.l.Addr().String())
}

//go:embed *.html
var webUI embed.FS

func New(listen string, indexer indexer.Interface, registry *registry.Registry, options ...ServerOption) (*Server, error) {
	var cfg serverConfig
	if err := cfg.apply(options...); err != nil {
		return nil, err
	}
	var err error

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	server := &http.Server{
		Handler:      r,
		WriteTimeout: cfg.apiWriteTimeout,
		ReadTimeout:  cfg.apiReadTimeout,
	}
	s := &Server{server, l}

	// Resource handler
	h := newHandler(indexer, registry)

	// Client routes
	cidR := mux.NewRouter().StrictSlash(true)
	cidR.HandleFunc("/{cid}", h.findCid).Methods(http.MethodGet)
	corCidR := handlers.CORS(handlers.AllowedOrigins([]string{"*"}))(cidR)

	mhR := mux.NewRouter().StrictSlash(true)
	mhR.HandleFunc("/{multihash}", h.find).Methods(http.MethodGet)
	mhR.HandleFunc("/", h.findBatch).Methods(http.MethodPost)
	corMhR := handlers.CORS(handlers.AllowedOrigins([]string{"*"}))(mhR)

	r := mux.NewRouter().StrictSlash(true)
	r.Handle("/cid", corCidR)
	r.Handle("/multihash", corMhR)

	r.HandleFunc("/health", h.health).Methods(http.MethodGet)
	r.Handle("/", http.FileServer(http.FS(webUI)))

	r.HandleFunc("/providers", h.listProviders).Methods(http.MethodGet)
	r.HandleFunc("/providers/{providerid}", h.getProvider).Methods(http.MethodGet)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("finder http server listening", "listen_addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
