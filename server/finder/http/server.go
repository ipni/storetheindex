package httpfinderserver

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"
	"text/template"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/server/reframe"
	xnet "golang.org/x/net/netutil"
)

var log = logging.Logger("indexer/finder")

type Server struct {
	server *http.Server
	l      net.Listener
	h      *httpHandler
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.l.Addr().String())
}

//go:embed *.html
var webUI embed.FS

func acceptsAnyOrJson(req *http.Request, _ *mux.RouteMatch) bool {
	values := req.Header.Values("Accept")
	// If there is no `Accept` header values, be forgiving and fall back on JSON.
	if len(values) == 0 {
		return true
	}
	accepts, err := acceptsAnyOf(req, mediaTypeJson, mediaTypeAny)
	if err != nil {
		log.Debugw("invalid accept header", "err", err)
		return false
	}
	return accepts
}

func acceptsAnyOrNDJsonOrJson(req *http.Request, _ *mux.RouteMatch) bool {
	values := req.Header.Values("Accept")
	// If there is no `Accept` header values, be forgiving and fall back on JSON.
	if len(values) == 0 {
		return true
	}
	accepts, err := acceptsAnyOf(req, mediaTypeJson, mediaTypeNDJson, mediaTypeAny)
	if err != nil {
		log.Debugw("invalid accept header", "err", err)
		return false
	}
	return accepts
}

func New(listen string, indexer indexer.Interface, registry *registry.Registry, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	if opts.maxConns > 0 {
		// Limit the number of open connections to the listener.
		l = xnet.LimitListener(l, opts.maxConns)
	}

	// Resource handler
	h := newHandler(indexer, registry, opts.indexCounts)

	// Compile index template.
	t, err := template.ParseFS(webUI, "index.html")
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err = t.Execute(&buf, struct {
		URL string
	}{
		URL: opts.homepageURL,
	}); err != nil {
		return nil, err
	}

	// Client routes
	r := mux.NewRouter().StrictSlash(true)
	compileTime := time.Now()
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "index.html", compileTime, bytes.NewReader(buf.Bytes()))
	}).Methods(http.MethodGet)

	r.HandleFunc("/cid/{cid}", h.findCid).Methods(http.MethodGet).MatcherFunc(acceptsAnyOrNDJsonOrJson)
	r.HandleFunc("/multihash/{multihash}", h.find).Methods(http.MethodGet).MatcherFunc(acceptsAnyOrNDJsonOrJson)
	r.HandleFunc("/multihash", h.findBatch).Methods(http.MethodPost).MatcherFunc(acceptsAnyOrJson)
	r.HandleFunc("/health", h.health).Methods(http.MethodGet)
	r.HandleFunc("/providers", h.listProviders).Methods(http.MethodGet).MatcherFunc(acceptsAnyOrJson)
	r.HandleFunc("/providers/{providerid}", h.getProvider).Methods(http.MethodGet).MatcherFunc(acceptsAnyOrJson)
	r.HandleFunc("/stats", h.getStats).Methods(http.MethodGet).MatcherFunc(acceptsAnyOrJson)

	reframeHandler := reframe.NewReframeHTTPHandler(indexer, registry)
	r.HandleFunc("/reframe", reframeHandler)

	cors := handlers.CORS(handlers.AllowedOrigins([]string{"*"}))
	server := &http.Server{
		Handler:      cors(r),
		WriteTimeout: opts.writeTimeout,
		ReadTimeout:  opts.readTimeout,
	}
	s := &Server{
		server: server,
		l:      l,
		h:      h,
	}

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("finder http server listening", "listen_addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) RefreshStats() {
	s.h.finderHandler.RefreshStats()
}

func (s *Server) Close() error {
	log.Info("finder http server shutdown")
	s.h.finderHandler.Close()
	return s.server.Shutdown(context.Background())
}
