package httpfinderserver

import (
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/server/reframe"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	xnet "golang.org/x/net/netutil"
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

	// Limit the number of open connections to the listener.
	l = xnet.LimitListener(l, cfg.maxConns)

	// Resource handler
	h := newHandler(indexer, registry)

	// Client routes
	r := mux.NewRouter().StrictSlash(true)
	r.Handle("/", http.FileServer(http.FS(webUI)))
	r.HandleFunc("/cid/{cid}", h.findCid).Methods(http.MethodGet)
	r.HandleFunc("/multihash/{multihash}", h.find).Methods(http.MethodGet)
	r.HandleFunc("/multihash", h.findBatch).Methods(http.MethodPost)
	r.HandleFunc("/health", h.health).Methods(http.MethodGet)
	r.HandleFunc("/providers", h.listProviders).Methods(http.MethodGet)
	r.HandleFunc("/providers/{providerid}", h.getProvider).Methods(http.MethodGet)
	r.HandleFunc("/stats", h.getStats).Methods(http.MethodGet)

	reframeHandler := reframe.NewReframeHTTPHandler(indexer, registry)
	r.HandleFunc("/reframe", reframeHandler)

	cors := handlers.CORS(handlers.AllowedOrigins([]string{"*"}))
	server := &http.Server{
		Handler:      cors(r),
		WriteTimeout: cfg.apiWriteTimeout,
		ReadTimeout:  cfg.apiReadTimeout,
	}
	s := &Server{server, l}

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("finder http server listening", "listen_addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
