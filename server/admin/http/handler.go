package adminserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/importer"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gorilla/mux"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type adminHandler struct {
	ctx      context.Context
	indexer  indexer.Interface
	ingester *ingest.Ingester
	reg      *registry.Registry
}

func newHandler(ctx context.Context, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry) *adminHandler {
	return &adminHandler{
		ctx:      ctx,
		indexer:  indexer,
		ingester: ingester,
		reg:      reg,
	}
}

const importBatchSize = 256

// ----- ingest handlers -----

func (h *adminHandler) allowProvider(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
	if !ok {
		return
	}
	log.Infow("Allowing provider", "provider", provID.String())
	h.reg.AllowProvider(provID)
	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) blockProvider(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
	if !ok {
		return
	}
	log.Infow("Blocking provider", "provider", provID.String())
	h.reg.BlockProvider(provID)
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

	data, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var syncAddr multiaddr.Multiaddr
	err = syncAddr.UnmarshalJSON(data)
	if err != nil {
		log.Errorw("Cannot unmarshal sync addr", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Infow("Syncing with provider", "provider", provID.String(), "address", syncAddr)

	// Start the sync, but do not wait for it to complete.
	//
	// We can include an ingestion API to check the latest sync for
	// a provider in the indexer. This would show if the indexer
	// has finally synced or not.
	_, err = h.ingester.Sync(h.ctx, provID, syncAddr)
	if err != nil {
		msg := "Cannot sync with provider"
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadGateway)
		return
	}

	// Return (202) Accepted
	w.WriteHeader(http.StatusAccepted)
}

// ----- import handlers -----

func (h *adminHandler) importManifest(w http.ResponseWriter, r *http.Request) {
	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
	if !ok {
		return
	}

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
	vars := mux.Vars(r)
	provID, ok := decodeProviderID(vars["provider"], w)
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

	fmt.Println("file:", fileName)
	fmt.Println("contextID:", contextID)
	fmt.Println("metadata:", metadata)

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
