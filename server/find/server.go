package find

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"text/template"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/internal/httpserver"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/server/find/handler"
	"github.com/ipni/storetheindex/server/reframe"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	xnet "golang.org/x/net/netutil"
)

var (
	log     = logging.Logger("indexer/find")
	newline = []byte("\n")
)

type Server struct {
	server    *http.Server
	listener  net.Listener
	handler   *handler.Handler
	healthMsg string
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

//go:embed *.html
var webUI embed.FS

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
	compileTime := time.Now()

	mux := http.NewServeMux()
	server := &http.Server{
		Handler:      mux,
		WriteTimeout: opts.writeTimeout,
		ReadTimeout:  opts.readTimeout,
	}
	s := &Server{
		server:   server,
		listener: l,
		handler:  handler.New(indexer, registry),
	}

	s.healthMsg = "ready"
	if opts.version != "" {
		s.healthMsg += " " + opts.version
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Do not fall back on web-ui on unknwon paths. Instead, strictly check the path and
		// return 404 on anything but "/" and "index.html". Otherwise, paths that are supported by
		// some backends and not others, like "/metadata" will return text/html.
		switch r.URL.Path {
		case "/", "/index.html":
			enableCors(w)
			if !httpserver.MethodOK(w, r, http.MethodGet) {
				return
			}
			http.ServeContent(w, r, "index.html", compileTime, bytes.NewReader(buf.Bytes()))
		default:
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		}
	})
	mux.HandleFunc("/cid/", s.findCid)
	mux.HandleFunc("/multihash", s.findBatch)
	mux.HandleFunc("/multihash/", s.findMultihash)
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/providers", s.listProviders)
	mux.HandleFunc("/providers/", s.getProvider)
	mux.HandleFunc("/stats", s.getStats)

	reframeHandler := reframe.NewReframeHTTPHandler(indexer, registry)
	mux.HandleFunc("/reframe", reframeHandler)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("find http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) RefreshStats() {
	s.handler.RefreshStats()
}

func (s *Server) Close() error {
	log.Info("find http server shutdown")
	s.handler.Close()
	return s.server.Shutdown(context.Background())
}

func enableCors(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func (s *Server) findCid(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	match, ok := acceptsAnyOf(w, r, false, mediaTypeNDJson, mediaTypeJson, mediaTypeAny)
	if !ok {
		return
	}
	// Explicitly accepts NDJson.
	stream := match == mediaTypeNDJson

	cidVar := path.Base(r.URL.Path)
	c, err := cid.Decode(cidVar)
	if err != nil {
		log.Errorw("error decoding cid", "cid", cidVar, "err", err)
		httpserver.HandleError(w, err, "find")
		return
	}
	s.getIndexes(w, []multihash.Multihash{c.Hash()}, stream)
}

func (s *Server) findMultihash(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	match, ok := acceptsAnyOf(w, r, false, mediaTypeNDJson, mediaTypeJson, mediaTypeAny)
	if !ok {
		return
	}
	// Explicitly accepts NDJson.
	stream := match == mediaTypeNDJson

	mhVar := path.Base(r.URL.Path)
	m, err := multihash.FromB58String(mhVar)
	if err != nil {
		log.Errorw("error decoding multihash", "multihash", mhVar, "err", err)
		httpserver.HandleError(w, err, "find")
		return
	}
	s.getIndexes(w, []multihash.Multihash{m}, stream)
}

func (s *Server) findBatch(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	if _, ok := acceptsAnyOf(w, r, false, mediaTypeJson, mediaTypeAny); !ok {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading get batch request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	req, err := model.UnmarshalFindRequest(body)
	if err != nil {
		log.Errorw("error unmarshalling get batch request", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	s.getIndexes(w, req.Multihashes, false)
}

func (s *Server) listProviders(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	if _, ok := acceptsAnyOf(w, r, false, mediaTypeJson, mediaTypeAny); !ok {
		return
	}

	data, err := s.handler.ListProviders()
	if err != nil {
		log.Errorw("cannot list providers", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (s *Server) getProvider(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	if _, ok := acceptsAnyOf(w, r, false, mediaTypeJson, mediaTypeAny); !ok {
		return
	}

	providerID, err := getProviderID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, err := s.handler.GetProvider(providerID)
	if err != nil {
		log.Error("cannot get provider", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	if len(data) == 0 {
		http.Error(w, "provider not found", http.StatusNotFound)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	if _, ok := acceptsAnyOf(w, r, false, mediaTypeJson, mediaTypeAny); !ok {
		return
	}

	data, err := s.handler.GetStats()
	switch {
	case err != nil:
		log.Errorw("cannot get stats", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	case len(data) == 0:
		log.Warn("processing stats")
		http.Error(w, "processing", http.StatusTeapot)
	default:
		httpserver.WriteJsonResponse(w, http.StatusOK, data)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	w.Header().Set("Cache-Control", "no-cache")
	http.Error(w, s.healthMsg, http.StatusOK)
}

func (s *Server) getIndexes(w http.ResponseWriter, mhs []multihash.Multihash, stream bool) {
	if len(mhs) != 1 && stream {
		log.Errorw("Streaming response is not supported for batch find")
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	startTime := time.Now()
	var found bool
	defer func() {
		msecPerMh := coremetrics.MsecSince(startTime) / float64(len(mhs))
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(tag.Insert(metrics.Method, "http"), tag.Insert(metrics.Found, fmt.Sprintf("%v", found))),
			stats.WithMeasurements(metrics.FindLatency.M(msecPerMh)))
	}()

	response, err := s.handler.Find(mhs)
	if err != nil {
		httpserver.HandleError(w, err, "get")
		return
	}

	// If no info for any multihashes, then 404
	if len(response.MultihashResults) == 0 {
		http.Error(w, "no results for query", http.StatusNotFound)
		return
	}

	if stream {
		log := log.With("mh", mhs[0].B58String())
		pr := response.MultihashResults[0].ProviderResults
		if len(pr) == 0 {
			http.Error(w, "no results for query", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", mediaTypeNDJson)
		w.Header().Set("Connection", "Keep-Alive")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		flusher, flushable := w.(http.Flusher)
		encoder := json.NewEncoder(w)
		var count int
		for _, result := range pr {
			if err := encoder.Encode(result); err != nil {
				log.Errorw("Failed to encode streaming response", "err", err)
				break
			}
			if _, err := w.Write(newline); err != nil {
				log.Errorw("failed to write newline while streaming results", "err", err)
				break
			}
			// TODO: optimise the number of time we call flush based on some time-based or result
			//       count heuristic.
			if flushable {
				flusher.Flush()
			}
			count++
		}
		if count == 0 {
			log.Errorw("Failed to encode results; falling back on not found", "resultsCount", len(pr))
			http.Error(w, "no results for query", http.StatusNotFound)
			return
		}
		found = true
		return
	}

	rb, err := model.MarshalFindResponse(response)
	if err != nil {
		log.Errorw("failed marshalling query response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	found = true
	httpserver.WriteJsonResponse(w, http.StatusOK, rb)
}

func getProviderID(r *http.Request) (peer.ID, error) {
	providerID, err := peer.Decode(path.Base(r.URL.Path))
	if err != nil {
		return providerID, fmt.Errorf("cannot decode provider id: %s", err)
	}
	return providerID, nil
}
