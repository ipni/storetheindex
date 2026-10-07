package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/admin/model"
	"github.com/ipni/storetheindex/internal/freeze"
	"github.com/ipni/storetheindex/internal/httpserver"
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

// ----- assignment handlers -----
func (h *adminHandler) listAssignedPeers(w http.ResponseWriter, r *http.Request) {

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

	writeJSON(w, apiAssigned, "Error marshaling assigned list")
}

func (h *adminHandler) listPreferredPeers(w http.ResponseWriter, r *http.Request) {

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

	writeJSON(w, preferred, "Error marshaling preferred list")
}

func (h *adminHandler) handoffPeer(w http.ResponseWriter, r *http.Request) {

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
}

func (h *adminHandler) assignPeer(w http.ResponseWriter, r *http.Request) {

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}

	err := h.reg.AssignPeer(peerID)
	if err != nil {
		assignError(w, err)
		return
	}
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
}

// ----- ingest handlers -----

func (h *adminHandler) allowPeer(w http.ResponseWriter, r *http.Request) {

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log.Infow("Allowing peer to publish and provide content", "peer", peerID)
	if h.reg.AllowPeer(peerID) {
		log.Infow("Update config to persist allowing peer", "peerr", peerID)
	}
}

func (h *adminHandler) blockPeer(w http.ResponseWriter, r *http.Request) {

	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	log.Infow("Blocking peer from publishing or providing content", "peer", peerID.String())
	if h.reg.BlockPeer(peerID) {
		log.Infow("Update config to persist blocking peer", "provider", peerID)
	}
}

func (h *adminHandler) markAdProcessed(w http.ResponseWriter, r *http.Request) {
	peerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}
	pinfo, found := h.reg.ProviderInfo(peerID)
	if !found {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	cidStr := r.URL.Query().Get("cid")
	stopCid, err := cid.Decode(cidStr)
	if err != nil {
		log.Errorw("error decoding cid", "cid", cidStr, "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = h.ingester.MarkAdProcessed(pinfo.Publisher, stopCid)
	if err != nil {
		log.Errorw("Failed to explicitly mark adas processed", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	log.Infow("Explicitly marked advertisement as processed", "adCid", stopCid, "provider", peerID)
}

func (h *adminHandler) handlePostSyncs(w http.ResponseWriter, r *http.Request) {

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

	var adCid cid.Cid
	adCidStr := query.Get("adcid")
	if adCidStr != "" {
		var err error
		adCid, err = cid.Decode(adCidStr)
		if err != nil {
			msg := "cannot decode advertisement cid"
			log.Errorw(msg, "value", adCidStr, "err", err)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
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

	// Start the sync, but do not wait for it to complete.
	h.pendingSyncsLock.Lock()
	if _, ok := h.pendingSyncsPeers[peerID]; ok {
		h.pendingSyncsLock.Unlock()
		log.Info("Manual sync ignored because another sync is in progress")
		msg := fmt.Sprintf("Peer %s has already a sync in progress", peerID.String())
		http.Error(w, msg, http.StatusConflict)
		return
	}
	h.pendingSyncsPeers[peerID] = struct{}{}
	h.pendingSyncsLock.Unlock()

	log.Info("Syncing with peer")
	h.pendingSyncs.Add(1)
	go func() {
		peerInfo := peer.AddrInfo{
			ID: peerID,
		}
		if syncAddr != nil {
			peerInfo.Addrs = []multiaddr.Multiaddr{syncAddr}
		}

		_, err := h.ingester.Sync(h.ctx, peerInfo, int(depth), resync, adCid)
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

	slices.Sort(peers)
	marshalled, err := json.Marshal(peers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	httpserver.WriteJsonResponse(w, http.StatusOK, marshalled)
}

func (h *adminHandler) importProviders(w http.ResponseWriter, r *http.Request) {

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
}

func (h *adminHandler) reloadConfig(w http.ResponseWriter, r *http.Request) {

	errChan := make(chan error)
	h.reloadErrChan <- errChan
	err := <-errChan
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ----- admin handlers -----

func (h *adminHandler) removeProvider(w http.ResponseWriter, r *http.Request) {

	providerID, ok := decodePeerID(path.Base(r.URL.Path), w)
	if !ok {
		return
	}

	query := r.URL.Query()
	var block bool
	blockStr := query.Get("block")
	if blockStr != "" {
		var err error
		block, err = strconv.ParseBool(blockStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("bad value for block: %s", blockStr), http.StatusBadRequest)
			return
		}
	}

	log.Infow("Removing provider", "provider", providerID, "block", block)

	err := h.reg.RemoveProvider(r.Context(), providerID, block)
	if err != nil {
		log.Errorw("Failed to remove provider", "err", err, "provider", providerID)
		if strings.HasSuffix(err.Error(), "not found") {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}
}

func (h *adminHandler) freeze(w http.ResponseWriter, r *http.Request) {

	err := h.reg.Freeze()
	if err != nil {
		if errors.Is(err, freeze.ErrNoFreeze) {
			log.Infow("Cannot freeze indexer", "reason", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Errorw("Cannot freeze indexer", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
}

func (h *adminHandler) status(w http.ResponseWriter, r *http.Request) {

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

	writeJSON(w, status, "Error marshaling status")
}

func (h *adminHandler) healthCheckHandler(w http.ResponseWriter, r *http.Request) {

	if err := healthCheckValueStore(h); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := w.Write([]byte("\"OK\"")); err != nil {
		log.Errorw("Cannot write HealthCheck response:", "err", err)
	}
}

// ----- Telemetry routes -----

func (h *adminHandler) listTelemetry(w http.ResponseWriter, r *http.Request) {

	ingestRates := h.ingester.GetAllIngestRates()
	if len(ingestRates) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	writeJSON(w, ingestRates, "Error marshaling telemetry data")
}

func (h *adminHandler) getTelemetry(w http.ResponseWriter, r *http.Request) {

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

	writeJSON(w, ingestRate, "Error marshaling telemetry data")
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

// ----- Metering routes -----

func (h *adminHandler) meteringStats(w http.ResponseWriter, r *http.Request) {
	// An empty provider list asks for totals only. The store does not read provider rows.
	switch report, err := h.indexer.MeteringAllStats(r.Context(), []peer.ID{}); {
	case err != nil:
		writeMeteringErr(w, err, "Error reading metering stats")
	case report == nil:
		w.WriteHeader(http.StatusNoContent)
	default:
		writeJSON(w, report.CompletedScanStats, "Error marshaling metering stats")
	}
}

func (h *adminHandler) meteringProviders(w http.ResponseWriter, r *http.Request) {
	switch report, err := h.indexer.MeteringAllStats(r.Context(), nil); {
	case err != nil:
		writeMeteringErr(w, err, "Error reading metering stats")
	case report == nil:
		w.WriteHeader(http.StatusNoContent)
	default:
		writeJSON(w, report, "Error marshaling metering stats")
	}
}

func (h *adminHandler) meteringProvider(w http.ResponseWriter, r *http.Request) {
	providerID, ok := decodePeerID(r.PathValue("providerID"), w)
	if !ok {
		return
	}

	switch report, err := h.indexer.MeteringAllStats(r.Context(), []peer.ID{providerID}); {
	case err != nil:
		writeMeteringErr(w, err, "Error reading metering stats")
	case report == nil || len(report.Providers) == 0:
		w.WriteHeader(http.StatusNoContent)
	default:
		writeJSON(w, report.Providers[0], "Error marshaling metering stats")
	}
}

func (h *adminHandler) meteringScan(w http.ResponseWriter, r *http.Request) {
	switch status, err := h.indexer.MeteringScanStatus(r.Context(), nil); {
	case err != nil:
		writeMeteringErr(w, err, "Error reading metering scan status")
	default:
		writeJSON(w, scanStatusBody(status), "Error marshaling metering scan status")
	}
}

func (h *adminHandler) meteringTriggerScan(w http.ResponseWriter, r *http.Request) {
	switch err := h.indexer.MeteringTriggerScan(r.Context()); {
	case err != nil:
		writeMeteringErr(w, err, "Error triggering metering scan")
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

func (h *adminHandler) meteringCancelScan(w http.ResponseWriter, r *http.Request) {
	reason := r.URL.Query().Get("reason")

	switch err := h.indexer.MeteringCancelScan(r.Context(), reason); {
	case err != nil:
		writeMeteringErr(w, err, "Error cancelling metering scan")
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

func (h *adminHandler) meteringProviderScan(w http.ResponseWriter, r *http.Request) {
	providerID, ok := decodePeerID(r.PathValue("providerID"), w)
	if !ok {
		return
	}

	switch status, err := h.indexer.MeteringScanStatus(r.Context(), []peer.ID{providerID}); {
	case err != nil:
		writeMeteringErr(w, err, "Error reading metering scan status")
	default:
		writeJSON(w, scanStatusBody(status), "Error marshaling metering scan status")
	}
}

// writeMeteringErr writes the HTTP response for a failed metering call.
// Unsupported metering is 501. A scan that is already running, or not running
// when cancel is requested, is 409.
func writeMeteringErr(w http.ResponseWriter, err error, msg string) {
	switch {
	case errors.Is(err, indexer.ErrMeteringNotSupported):
		http.Error(w, err.Error(), http.StatusNotImplemented)
	case errors.Is(err, indexer.ErrScanInProgress), errors.Is(err, indexer.ErrScanNotInProgress):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		log.Errorw(msg, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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

// writeJSON marshals body and writes it as JSON. A marshal failure is a 500
// with an empty body.
func writeJSON(w http.ResponseWriter, body any, msg string) {
	data, err := json.Marshal(body)
	if err != nil {
		log.Errorw(msg, "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, data)
}

// scanStatusBody is the JSON body for a scan status. A scan that has not been
// recorded is only State "none". A running, finished, or failed scan is the
// full status, including the counters it produced.
func scanStatusBody(status *indexer.ScanStatus) any {
	if status == nil || status.State == "" || status.State == indexer.ScanStateNone {
		return struct {
			State indexer.ScanState
		}{State: indexer.ScanStateNone}
	}
	return status
}
