package node

import (
	"errors"
	"net/http"

	"github.com/filecoin-project/storetheindex/importer"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// NOTE: Considering sending a JSON in the body of these handlers
// to give additional input about the error to the client.

func (n *Node) ImportManifestHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.IDB58Decode(m)
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
	imp := importer.NewManifestImporter(file, miner)
	go imp.Read(r.Context(), out, errOut)

	for c := range out {
		err = n.importCallback(c, cid.Undef, miner)
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

func (n *Node) ImportCidListHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.IDB58Decode(m)
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
	imp := importer.NewCidListImporter(file, miner)
	go imp.Read(r.Context(), out, errOut)

	for c := range out {
		err = n.importCallback(c, cid.Undef, miner)
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

func (n *Node) importCallback(c, piece cid.Cid, prov peer.ID) error {
	// Disregard empty Cids.
	if c == cid.Undef {
		return nil
	}
	// NOTE: We disregard errors for now
	isNew, err := n.primary.Put(c, prov, piece)
	if err != nil {
		log.Errorw("primary storage Put returned error", "err", err, "cid", c)
		return errors.New("failed to store in primary storage")
	}
	// TODO: Change to Debug
	log.Infow("Imported successfully", "new", isNew, "cid", c)
	return nil
}
