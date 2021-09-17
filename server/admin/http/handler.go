package adminserver

import (
	"context"
	"errors"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/importer"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/gorilla/mux"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type adminHandler struct {
	ctx      context.Context
	indexer  indexer.Interface
	ingester ingest.Ingester
}

func newHandler(ctx context.Context, indexer indexer.Interface, ingester ingest.Ingester) *adminHandler {
	return &adminHandler{
		ctx:      ctx,
		indexer:  indexer,
		ingester: ingester,
	}
}

// ----- ingest handlers -----

func (h *adminHandler) subscribe(w http.ResponseWriter, r *http.Request) {
	if ret := h.checkIngester(w, r); ret {
		return
	}
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
	if !ok {
		return
	}
	log.Infow("Subscribing to provider", "provider", provID.String())
	err := h.ingester.Subscribe(h.ctx, provID)
	if err != nil {
		msg := "Cannot subscribe to provider"
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
	}

	// Return OK
	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) unsubscribe(w http.ResponseWriter, r *http.Request) {
	if ret := h.checkIngester(w, r); ret {
		return
	}
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
	if !ok {
		return
	}
	log.Infow("Unsubscribing to provider", "provider", provID.String())
	err := h.ingester.Unsubscribe(h.ctx, provID)
	if err != nil {
		msg := "Cannot unsubscribe to provider"
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
	}

	// Return OK
	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) sync(w http.ResponseWriter, r *http.Request) {
	if ret := h.checkIngester(w, r); ret {
		return
	}
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
	if !ok {
		return
	}
	log.Infow("Syncing with provider", "provider", provID.String())

	// We accept the request but do nothing with the channel.
	// We can include an ingestion API to check the latest sync for
	// a provider in the indexer. This would show if the indexer
	// has finally synced or not.
	_, err := h.ingester.Sync(h.ctx, provID)
	if err != nil {
		msg := "Cannot sync with provider"
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusInternalServerError)
	}

	// Return (202) Accepted
	w.WriteHeader(http.StatusAccepted)
}

// ----- import handlers -----

func (h *adminHandler) importManifest(w http.ResponseWriter, r *http.Request) {
	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["minerid"], w)
	if !ok {
		return
	}
	log.Infow("Import manifest for provider", "miner", provID.String())
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Error("Error reading file")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	out := make(chan multihash.Multihash)
	errOut := make(chan error, 1)
	go importer.ReadManifest(h.ctx, file, out, errOut)

	value := indexer.MakeValue(provID, 0, nil)
	for c := range out {
		err = h.importCallback(c, value)
		if err != nil {
			log.Errorw("Import callback failure", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	err = <-errOut
	if err == nil {
		log.Info("Success importing")
		w.WriteHeader(http.StatusOK)
	} else {
		log.Errorw("Import failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *adminHandler) importCidList(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["minerid"], w)
	if !ok {
		return
	}
	log.Infow("Import multihash list for provider", "miner", provID.String())
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Error("Cannot read file")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	out := make(chan multihash.Multihash)
	errOut := make(chan error, 1)
	go importer.ReadCids(r.Context(), file, out, errOut)

	value := indexer.MakeValue(provID, 0, nil)
	for c := range out {
		err = h.importCallback(c, value)
		if err != nil {
			log.Errorw("Import callback failed", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	err = <-errOut
	if err == nil {
		log.Info("Success importing")
		w.WriteHeader(http.StatusOK)
	} else {
		log.Errorw("Failed importing", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// ----- admin handlers -----

func (h *adminHandler) healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	// TODO: Report on indexer core health?
	_, err := w.Write([]byte("\"OK\""))
	if err != nil {
		log.Errorw("Cannot write HealthCheck response:", err)
		return
	}
}

// ----- utility functions -----

func (h *adminHandler) checkIngester(w http.ResponseWriter, r *http.Request) bool {
	if h.ingester == nil {
		msg := "No ingester set in indexer"
		log.Error(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return true
	}
	return false
}

func (h *adminHandler) importCallback(m multihash.Multihash, value indexer.Value) error {
	// NOTE: We disregard errors for now
	_, err := h.indexer.Put(m, value)
	if err != nil {
		log.Errorw("Indexer Put returned error", "err", err, "multihash", m.B58String())
		return errors.New("failed to store in indexer")
	}
	// TODO: Change to Debug
	log.Infow("Imported successfully", "multihash", m.B58String())
	return nil
}

func decodeProviderID(id string, w http.ResponseWriter) (peer.ID, bool) {
	provID, err := peer.Decode(id)
	if err != nil {
		msg := "Cannot decode provider id"
		log.Errorw(msg, "id", id, "err", err)
		http.Error(w, msg, http.StatusBadRequest)
		return provID, false
	}
	return provID, true
}
