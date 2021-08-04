package handler

import (
	"errors"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/storetheindex/internal/importer"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("admin_handler")

type AdminHandler struct {
	engine *indexer.Engine
}

func New(engine *indexer.Engine) *AdminHandler {
	return &AdminHandler{
		engine: engine,
	}
}

func (h *AdminHandler) ImportManifest(w http.ResponseWriter, r *http.Request) {
	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.Decode(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID", "miner", m, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Import manifest for provider", "miner", m)
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Errorw("error reading file")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	out := make(chan cid.Cid)
	errOut := make(chan error, 1)
	go importer.ReadManifest(r.Context(), file, out, errOut)

	entry := entry.MakeValue(miner, 0, nil)
	for c := range out {
		err = h.importCallback(c, entry)
		if err != nil {
			log.Errorw("import callback error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	err = <-errOut
	if err == nil {
		log.Info("success importing")
		w.WriteHeader(http.StatusOK)
	} else {
		log.Errorw("error importing", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *AdminHandler) ImportCidList(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.Decode(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID", "miner", m, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Import cidlist for provider", "miner", m)
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Error("error reading file")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	out := make(chan cid.Cid)
	errOut := make(chan error, 1)
	go importer.ReadCids(r.Context(), file, out, errOut)

	value := entry.MakeValue(miner, 0, nil)
	for c := range out {
		err = h.importCallback(c, value)
		if err != nil {
			log.Errorw("import callback error", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	err = <-errOut
	if err == nil {
		log.Info("success importing")
		w.WriteHeader(http.StatusOK)
	} else {
		log.Errorw("error importing", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *AdminHandler) importCallback(c cid.Cid, value entry.Value) error {
	// Disregard empty Cids.
	if c == cid.Undef {
		return nil
	}
	// NOTE: We disregard errors for now
	_, err := h.engine.Put(c, value)
	if err != nil {
		log.Errorw("indexer Put returned error", "err", err, "cid", c)
		return errors.New("failed to store in indexer")
	}
	// TODO: Change to Debug
	log.Infow("Imported successfully", "cid", c)
	return nil
}

func (h *AdminHandler) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	// TODO: Report on engine health?
	_, err := w.Write([]byte("\"OK\""))
	if err != nil {
		log.Errorw("cannot write HealthCheck response:", err)
		return
	}
}
