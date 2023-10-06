package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/admin/model"
	"github.com/ipni/storetheindex/internal/httpserver"
	"github.com/ipni/storetheindex/internal/importer"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type adminHandler struct {
	ctx               context.Context
	id                peer.ID
	indexer           indexer.Interface
	ingester          *ingest.Ingester
	reg               *registry.Registry
	reloadErrChan     chan<- chan error
	pendingSyncs      sync.WaitGroup
	pendingSyncsPeers map[peer.ID]struct{}
	pendingSyncsLock  sync.Mutex
}

func newHandler(ctx context.Context, id peer.ID, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, reloadErrChan chan<- chan error) *adminHandler {
	return &adminHandler{
		ctx:               ctx,
		id:                id,
		indexer:           indexer,
		ingester:          ingester,
		reg:               reg,
		reloadErrChan:     reloadErrChan,
		pendingSyncsPeers: make(map[peer.ID]struct{}),
	}
}

const importBatchSize = 256

// ----- assignment handlers -----
func (h *adminHandler) listAssignedPeers(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	publishers, continued, err := h.reg.ListAssignedPeers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if len(publishers) == 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	apiAssigned := make([]model.Assigned, len(publishers))
	for i := range publishers {
		apiAssigned[i].Publisher = publishers[i]
		apiAssigned[i].Continued = continued[i]
	}

	data, err := json.Marshal(apiAssigned)
	if err != nil {
		log.Errorw("Error marshaling assigned list", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (h *adminHandler) listPreferredPeers(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	preferred, err := h.reg.ListPreferredPeers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if len(preferred) == 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	data, err := json.Marshal(preferred)
	if err != nil {
		log.Errorw("Error marshaling preferred list", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (h *adminHandler) handoffPeer(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log := log.With("publisher", peerID)

	data, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("Failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if len(data) == 0 {
		http.Error(w, "missing handoff data", http.StatusBadRequest)
		return
	}

	var handoff model.Handoff
	err = json.Unmarshal(data, &handoff)
	if err != nil {
		log.Errorw("Cannot unmarshal handoff data", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log = log.With("from", handoff.FrozenID.String())

	frozenURL, err := url.Parse(handoff.FrozenURL)
	if err != nil {
		log.Errorw("Cannot parse handoff 'frozen' URL", "err", err, "url", handoff.FrozenURL)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.reg.Handoff(r.Context(), peerID, handoff.FrozenID, frozenURL)
	if err != nil {
		assignError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) assignPeer(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}

	err := h.reg.AssignPeer(peerID)
	if err != nil {
		assignError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func assignError(w http.ResponseWriter, err error) {
	log.Errorw("Cannot assign publisher to indexer", "err", err)
	switch {
	case errors.Is(err, registry.ErrNotAllowed), errors.Is(err, registry.ErrPublisherNotAllowed), errors.Is(err, registry.ErrCannotPublish):
		http.Error(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, registry.ErrNoAssigner):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, registry.ErrAlreadyAssigned):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (h *adminHandler) unassignPeer(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPut) {
		return
	}

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	ok, err := h.reg.UnassignPeer(peerID)
	if err != nil {
		log.Infow("Cannot unassign peer from indexer", "peer", peerID.String())
		if errors.Is(err, registry.ErrNoAssigner) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		} else {
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}
	if !ok {
		http.Error(w, "peer was not assigned", http.StatusNotFound)
		return
	}

	log.Infow("Unassigned publisher from indexer", "publisher", peerID)
	w.WriteHeader(http.StatusOK)
}

// ----- ingest handlers -----

func (h *adminHandler) allowPeer(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPut) {
		return
	}

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log.Infow("Allowing peer to publish and provide content", "peer", peerID)
	if h.reg.AllowPeer(peerID) {
		log.Infow("Update config to persist allowing peer", "peerr", peerID)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) blockPeer(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPut) {
		return
	}

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log.Infow("Blocking peer from publishing or providing content", "peer", peerID.String())
	if h.reg.BlockPeer(peerID) {
		log.Infow("Update config to persist blocking peer", "provider", peerID)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) handlePostSyncs(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	if h.ingester == nil {
		log.Warn("sync not available, ingester disabled")
		http.Error(w, "ingester disabled", http.StatusServiceUnavailable)
		return
	}

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log := log.With("peerID", peerID)

	query := r.URL.Query()
	var depth int64
	depthStr := query.Get("depth")
	if depthStr != "" {
		var err error
		depth, err = strconv.ParseInt(depthStr, 10, 0)
		if err != nil {
			log.Errorw("Cannot unmarshal recursion depth as integer", "depthStr", depthStr, "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log = log.With("depth", depth)
	}

	var resync bool
	resyncStr := query.Get("resync")
	if resyncStr != "" {
		var err error
		resync, err = strconv.ParseBool(resyncStr)
		if err != nil {
			log.Errorw("Cannot unmarshal flag resync as bool", "resync", resyncStr, "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log = log.With("resync", resync)
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("Failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	var syncAddr multiaddr.Multiaddr
	if len(data) != 0 {
		var v string
		err = json.Unmarshal(data, &v)
		if err == nil {
			syncAddr, err = multiaddr.NewMultiaddr(v)
		}
		if err != nil {
			log.Errorw("Cannot unmarshal sync multiaddr", "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log = log.With("address", syncAddr)
	}

	log.Info("Syncing with peer")

	// Start the sync, but do not wait for it to complete.
	h.pendingSyncs.Add(1)
	h.pendingSyncsLock.Lock()
	if _, ok := h.pendingSyncsPeers[peerID]; ok {
		h.pendingSyncsLock.Unlock()
		msg := fmt.Sprintf("Peer %s has already a sync in progress", peerID.String())
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	h.pendingSyncsPeers[peerID] = struct{}{}
	h.pendingSyncsLock.Unlock()

	go func() {
		peerInfo := peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{syncAddr},
		}
		_, err := h.ingester.Sync(h.ctx, peerInfo, int(depth), resync)
		if err != nil {
			log.Errorw("Cannot sync with peer", "err", err)
		}
		h.pendingSyncs.Done()

		h.pendingSyncsLock.Lock()
		delete(h.pendingSyncsPeers, peerID)
		h.pendingSyncsLock.Unlock()
	}()

	// Return (202) Accepted
	w.WriteHeader(http.StatusAccepted)
}

func (h *adminHandler) handleGetSyncs(w http.ResponseWriter, r *http.Request) {
	h.pendingSyncsLock.Lock()
	peers := make([]string, 0, len(h.pendingSyncsPeers))
	for k := range h.pendingSyncsPeers {
		peers = append(peers, k.String())
	}
	h.pendingSyncsLock.Unlock()

	sort.Strings(peers)
	marshalled, err := json.Marshal(peers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, marshalled)
}

func (h *adminHandler) sync(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handlePostSyncs(w, r)
	case http.MethodGet:
		h.handleGetSyncs(w, r)
	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func (h *adminHandler) importProviders(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading import providers request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	var params map[string][]byte
	err = json.Unmarshal(body, &params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	from, ok := params["indexer"]
	if !ok {
		http.Error(w, "missing indexer url in request", http.StatusBadRequest)
		return
	}

	fromURL := &url.URL{}
	err = fromURL.UnmarshalBinary(from)
	if err != nil {
		http.Error(w, "bad indexer url: "+err.Error(), http.StatusBadRequest)
		return
	}

	_, err = h.reg.ImportProviders(h.ctx, fromURL)
	if err != nil {
		msg := "Cannot get providers from other indexer"
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) reloadConfig(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	errChan := make(chan error)
	h.reloadErrChan <- errChan
	err := <-errChan
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ----- import handlers -----

func (h *adminHandler) importManifest(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	provID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading import manifest request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	fileName, contextID, metadata, err := getParams(body)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Errorw("Cannot open cidlist file", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	defer file.Close()

	out := make(chan multihash.Multihash, importBatchSize)
	errOut := make(chan error, 1)
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	go importer.ReadManifest(ctx, file, out, errOut)

	value := indexer.Value{
		ProviderID:    provID,
		ContextID:     contextID,
		MetadataBytes: metadata,
	}
	batchErr := batchIndexerEntries(importBatchSize, out, value, h.indexer)
	err = <-batchErr
	if err != nil {
		log.Errorf("Error putting entries in indexer: %s", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = <-errOut
	if err != nil {
		log.Errorw("Error reading manifest", "err", err)
		http.Error(w, fmt.Sprintf("error reading manifest: %s", err), http.StatusBadRequest)
		return
	}

	log.Info("Success importing")
	w.WriteHeader(http.StatusOK)
}

func getParams(data []byte) (string, []byte, []byte, error) {
	var params map[string][]byte
	err := json.Unmarshal(data, &params)
	if err != nil {
		return "", nil, nil, fmt.Errorf("cannot unmarshal import cidlist params: %s", err)
	}
	fileName, ok := params["file"]
	if !ok {
		return "", nil, nil, errors.New("missing file in request")
	}
	contextID, ok := params["context_id"]
	if !ok {
		return "", nil, nil, errors.New("missing context_id in request")
	}
	metadata, ok := params["metadata"]
	if !ok {
		return "", nil, nil, errors.New("missing metadata in request")
	}

	return string(fileName), contextID, metadata, nil
}

func (h *adminHandler) importCidList(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPost) {
		return
	}

	provID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log.Infow("Import multihash list for provider", "provider", provID.String())

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading import cidlist request", "err", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	fileName, contextID, metadata, err := getParams(body)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Errorw("Cannot open cidlist file", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	defer file.Close()

	out := make(chan multihash.Multihash, importBatchSize)
	errOut := make(chan error, 1)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go importer.ReadCids(ctx, file, out, errOut)

	value := indexer.Value{
		ProviderID:    provID,
		ContextID:     contextID,
		MetadataBytes: metadata,
	}
	batchErr := batchIndexerEntries(importBatchSize, out, value, h.indexer)
	err = <-batchErr
	if err != nil {
		log.Errorf("Error putting entries in indexer: %s", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = <-errOut
	if err != nil {
		log.Errorw("Error reading CID list", "err", err)
		http.Error(w, fmt.Sprintf("error reading cid list: %s", err), http.StatusBadRequest)
		return
	}

	log.Info("Success importing")
	w.WriteHeader(http.StatusOK)
}

// batchIndexerEntries read
func batchIndexerEntries(batchSize int, putChan <-chan multihash.Multihash, value indexer.Value, idxr indexer.Interface) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		puts := make([]multihash.Multihash, 0, batchSize)
		for m := range putChan {
			puts = append(puts, m)
			if len(puts) == batchSize {
				// Process full batch of puts
				if err := idxr.Put(value, puts...); err != nil {
					errChan <- err
					return
				}
				puts = puts[:0]

			}
		}

		if len(puts) != 0 {
			// Process any remaining puts
			if err := idxr.Put(value, puts...); err != nil {
				errChan <- err
				return
			}
		}
	}()

	return errChan
}

// ----- admin handlers -----

func (h *adminHandler) freeze(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodPut) {
		return
	}

	err := h.reg.Freeze()
	if err != nil {
		if errors.Is(err, registry.ErrNoFreeze) {
			log.Infow("Cannot freeze indexer", "reason", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Errorw("Cannot freeze indexer", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) status(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	var usage float64
	du, err := h.reg.ValueStoreUsage()
	if err != nil {
		log.Error(err)
		usage = -1.0
	} else if du != nil {
		usage = du.Percent
	}

	status := model.Status{
		Frozen: h.reg.Frozen(),
		ID:     h.id,
		Usage:  usage,
	}

	data, err := json.Marshal(status)
	if err != nil {
		log.Errorw("Error marshaling status", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (h *adminHandler) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	if err := healthCheckValueStore(h); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("\"OK\"")); err != nil {
		log.Errorw("Cannot write HealthCheck response:", "err", err)
	}
}

// ----- Telemetry routes -----

func (h *adminHandler) listTelemetry(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	ingestRates := h.ingester.GetAllIngestRates()
	if len(ingestRates) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	data, err := json.Marshal(ingestRates)
	if err != nil {
		log.Errorw("Error marshaling telemetry data", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

func (h *adminHandler) getTelemetry(w http.ResponseWriter, r *http.Request) {
	if !httpserver.MethodOK(w, r, http.MethodGet) {
		return
	}

	providerID, err := peer.Decode(path.Base(r.URL.Path))
	if err != nil {
		http.Error(w, "cannot decode provider id", http.StatusBadRequest)
		return
	}

	ingestRate, ok := h.ingester.GetIngestRate(providerID)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	data, err := json.Marshal(ingestRate)
	if err != nil {
		log.Errorw("Error marshaling telemetry data", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

// ----- utility functions -----

func decodePeerID(id string, w http.ResponseWriter) (peer.ID, bool) {
	peerID, err := peer.Decode(id)
	if err != nil {
		msg := "Cannot decode peer id"
		log.Errorw(msg, "id", id, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return peerID, false
	}
	return peerID, true
}

var healthCheckMH multihash.Multihash
var healthCheckValue indexer.Value

func init() {
	provider, err := peer.Decode("12D3KooWBUNzpAz1Jfvnaag1nHBi6gbG5Q1BQm8kCfcpmt7bGKa6")
	if err != nil {
		panic(err.Error())
	}
	healthCheckMH = multihash.Multihash("2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj")
	healthCheckValue = indexer.Value{
		ProviderID:    provider,
		ContextID:     []byte(healthCheckMH),
		MetadataBytes: []byte("healthcheck-metadata"),
	}
}

func healthCheckValueStore(h *adminHandler) error {
	if err := h.indexer.Put(healthCheckValue, healthCheckMH); err != nil {
		return fmt.Errorf("cannot write to valuestore: %s", err)
	}

	rval, present, err := h.indexer.Get(healthCheckMH)

	if err != nil {
		return fmt.Errorf("cannot get value from valuestore: %s", err)
	}
	if !present {
		return errors.New("health-check value not found in valuestore")
	}
	if !healthCheckValue.Equal(rval[0]) {
		return errors.New("value stored does not match value retrieved from valuestore")
	}
	if err = h.indexer.Remove(healthCheckValue, healthCheckMH); err != nil {
		return fmt.Errorf("unable to remove value from valuestore: %s", err)
	}
	return nil
}
