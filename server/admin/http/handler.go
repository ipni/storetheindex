package adminserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/importer"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gorilla/mux"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type adminHandler struct {
	ctx           context.Context
	indexer       indexer.Interface
	ingester      *ingest.Ingester
	reg           *registry.Registry
	reloadErrChan chan<- chan error
}

func newHandler(ctx context.Context, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, reloadErrChan chan<- chan error) *adminHandler {
	return &adminHandler{
		ctx:           ctx,
		indexer:       indexer,
		ingester:      ingester,
		reg:           reg,
		reloadErrChan: reloadErrChan,
	}
}

const importBatchSize = 256

// ----- ingest handlers -----

func (h *adminHandler) allowPeer(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peerID, ok := decodePeerID(vars["peer"], w)
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
	vars := mux.Vars(r)
	peerID, ok := decodePeerID(vars["peer"], w)
	if !ok {
		return
	}
	log.Infow("Blocking peer from publishing or providing content", "peer", peerID.String())
	if h.reg.BlockPeer(peerID) {
		log.Infow("Update config to persist blocking peer", "provider", peerID)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *adminHandler) sync(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	peerID, ok := decodePeerID(vars["peer"], w)
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
	//
	// TODO: Provide some way for the client to see if the indexer has synced.
	_, err = h.ingester.Sync(h.ctx, peerID, syncAddr, int(depth), resync)
	if err != nil {
		msg := "Cannot sync with peer"
		log.Errorw(msg, "err", err)
		http.Error(w, msg, http.StatusBadGateway)
		return
	}

	// Return (202) Accepted
	w.WriteHeader(http.StatusAccepted)
}

func (h *adminHandler) importProviders(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading import cidlist request", "err", err)
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
	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	vars := mux.Vars(r)
	provID, ok := decodePeerID(vars["provider"], w)
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
	provID, ok := decodePeerID(vars["provider"], w)
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
	if err := healthCheckValueStore(h); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("\"OK\"")); err != nil {
		log.Errorw("Cannot write HealthCheck response:", "err", err)
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
