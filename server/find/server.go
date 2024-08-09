package find

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path"
	"text/template"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/internal/httpserver"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	xnet "golang.org/x/net/netutil"
)

var log = logging.Logger("indexer/find")

type Server struct {
	server    *http.Server
	listener  net.Listener
	healthMsg string
	indexer   indexer.Interface
	registry  *registry.Registry
	stats     *cachedStats
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
		indexer:  indexer,
		registry: registry,
		stats:    newCachedStats(indexer, time.Hour),
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
	mux.HandleFunc("/multihash/", s.findMultihash)
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/providers", s.listProviders)
	mux.HandleFunc("/providers/", s.getProvider)
	mux.HandleFunc("/stats", s.getStats)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("find http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) RefreshStats() {
	s.stats.refresh()
}

func (s *Server) Close() error {
	log.Info("find http server shutdown")
	s.stats.close()
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
		var hexErr error
		m, hexErr = multihash.FromHexString(mhVar)
		if hexErr != nil {
			msg := "find: input is not a valid base58 or hex encoded multihash"
			log.Errorw(msg, "multihash", mhVar, "err", err, "hexErr", hexErr)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
	}
	s.getIndexes(w, []multihash.Multihash{m}, stream)
}

func (s *Server) listProviders(w http.ResponseWriter, r *http.Request) {
	enableCors(w)

	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	if _, ok := acceptsAnyOf(w, r, false, mediaTypeJson, mediaTypeAny); !ok {
		return
	}

	infos := s.registry.AllProviderInfo()

	responses := make([]model.ProviderInfo, len(infos))
	for i, pInfo := range infos {
		responses[i] = *registry.RegToApiProviderInfo(pInfo)
	}

	data, err := json.Marshal(responses)
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

	info, allowed := s.registry.ProviderInfo(providerID)
	if info == nil || !allowed || info.Inactive() {
		http.Error(w, "provider not found", http.StatusNotFound)
		return
	}
	rsp := registry.RegToApiProviderInfo(info)
	data, err := json.Marshal(rsp)
	if err != nil {
		log.Error("cannot get provider", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
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

	stats, err := s.stats.get()
	if err != nil {
		log.Errorw("cannot get stats", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	data, err := model.MarshalStats(&stats)
	if err != nil {
		log.Errorw("cannot marshal stats", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if len(data) == 0 {
		log.Warn("processing stats")
		http.Error(w, "processing", http.StatusTeapot)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
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
			stats.WithTags(tag.Insert(metrics.Found, fmt.Sprintf("%v", found))),
			stats.WithMeasurements(metrics.FindLatency.M(msecPerMh)))
	}()

	response, err := s.find(mhs)
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

// find reads from indexer core to populate a response from a list of
// multihashes.
func (s *Server) find(mhashes []multihash.Multihash) (*model.FindResponse, error) {
	results := make([]model.MultihashResult, 0, len(mhashes))
	provInfos := map[peer.ID]*registry.ProviderInfo{}

	for i := range mhashes {
		values, found, err := s.indexer.Get(mhashes[i])
		if err != nil {
			err = fmt.Errorf("failed to query multihash %s: %s", mhashes[i].B58String(), err)
			return nil, apierror.New(err, http.StatusInternalServerError)
		}
		if !found {
			continue
		}

		provResults := make([]model.ProviderResult, 0, len(values))
		for j := range values {
			iVal := values[j]
			provID := iVal.ProviderID
			pinfo := s.fetchProviderInfo(provID, iVal.ContextID, provInfos, true)
			if pinfo == nil {
				continue
			}

			// Adding the main provider
			provResult := model.ProviderResult{
				ContextID: iVal.ContextID,
				Metadata:  iVal.MetadataBytes,
				Provider: &peer.AddrInfo{
					ID:    provID,
					Addrs: pinfo.AddrInfo.Addrs,
				},
			}
			provResults = append(provResults, provResult)

			if pinfo.ExtendedProviders == nil {
				continue
			}

			epRecord := pinfo.ExtendedProviders

			// If override is set to true at the context level then the chain
			// level EPs should be ignored for this context ID
			override := false

			// Adding context-level EPs if they exist
			if contextualEpRecord, ok := epRecord.ContextualProviders[string(iVal.ContextID)]; ok {
				override = contextualEpRecord.Override
				for _, epInfo := range contextualEpRecord.Providers {
					// Skippng the main provider's record if its metadata is
					// nil or is the same as the one retrieved from the
					// indexer, because such EP record does not advertise any
					// new protocol.
					if epInfo.PeerID == provID &&
						(len(epInfo.Metadata) == 0 || bytes.Equal(epInfo.Metadata, iVal.MetadataBytes)) {
						continue
					}
					provResult := createExtendedProviderResult(epInfo, iVal)
					provResults = append(provResults, *provResult)

				}
			}

			if override {
				continue
			}

			// Adding chain-level EPs if such exist
			for _, epInfo := range epRecord.Providers {
				// Skippng the main provider's record if its metadata is nil or
				// is the same as the one retrieved from the indexer, because
				// such EP record does not advertise any new protocol.
				if epInfo.PeerID == provID &&
					(len(epInfo.Metadata) == 0 || bytes.Equal(epInfo.Metadata, iVal.MetadataBytes)) {
					continue
				}
				provResult := createExtendedProviderResult(epInfo, iVal)
				provResults = append(provResults, *provResult)
			}

		}

		// If there are no providers for this multihash, then do not return a
		// result for it.
		if len(provResults) == 0 {
			continue
		}

		// Add the result to the list of index results.
		results = append(results, model.MultihashResult{
			Multihash:       mhashes[i],
			ProviderResults: provResults,
		})
	}

	return &model.FindResponse{
		MultihashResults: results,
	}, nil
}

func (s *Server) fetchProviderInfo(provID peer.ID, contextID []byte, provAddrs map[peer.ID]*registry.ProviderInfo, removeProviderContext bool) *registry.ProviderInfo {
	// Lookup provider info for each unique provider, look in local map
	// before going to registry.
	pinfo, ok := provAddrs[provID]
	if ok {
		return pinfo
	}
	pinfo, allowed := s.registry.ProviderInfo(provID)
	if pinfo == nil && removeProviderContext {
		// If provider not in registry, then provider was deleted.
		// Tell the indexed core to delete the contextID for the
		// deleted provider. Delete the contextID from the core,
		// because there is no way to delete all records for the
		// provider without a scan of the entire core valuestore.
		go func(provID peer.ID, contextID []byte) {
			err := s.indexer.RemoveProviderContext(provID, contextID)
			if err != nil {
				log.Errorw("Error removing provider context", "err", err)
			}
		}(provID, contextID)
		// If provider not in registry, do not return in result.
		return nil
	}
	// Omit provider info if not allowed or marked as inactive.
	if !allowed || pinfo.Inactive() {
		return nil
	}
	provAddrs[provID] = pinfo
	return pinfo
}

func createExtendedProviderResult(epInfo registry.ExtendedProviderInfo, iVal indexer.Value) *model.ProviderResult {
	metadata := epInfo.Metadata
	if metadata == nil {
		metadata = iVal.MetadataBytes
	}

	return &model.ProviderResult{
		ContextID: iVal.ContextID,
		Metadata:  metadata,
		Provider: &peer.AddrInfo{
			ID:    epInfo.PeerID,
			Addrs: epInfo.Addrs,
		},
	}
}
