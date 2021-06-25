package node

import (
	"net/http"

	"github.com/adlrocha/indexer-node/importer"
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
		log.Errorw("error decoding miner id into peerID:", m, err)
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

	o := make(chan cid.Cid)
	d := make(chan error)
	i := importer.NewManifestImporter(file, miner)
	go i.Read(r.Context(), o, d)

	for {
		select {
		case c := <-o:
			n.importCallback(c, miner, cid.Cid{})
		case err := <-d:
			if err == nil {
				log.Infow("success importing")
				w.WriteHeader(http.StatusOK)
			} else {
				log.Errorw("error importing", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}

func (n *Node) ImportCidListHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.IDB58Decode(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID:", m, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Import cidlist for provider: ", m)
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Errorw("error reading file")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	o := make(chan cid.Cid)
	d := make(chan error)
	i := importer.NewCidListImporter(file, miner)
	go i.Read(r.Context(), o, d)

	for {
		select {
		case c := <-o:
			n.importCallback(c, miner, cid.Cid{})
		case err := <-d:
			if err == nil {
				log.Infow("success importing")
				w.WriteHeader(http.StatusOK)
			} else {
				log.Errorw("error importing", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}

func (n *Node) importCallback(c cid.Cid, prov peer.ID, piece cid.Cid) error {
	// Disregard empty Cids.
	empty := cid.Cid{}
	if c == empty {
		return nil
	}
	// NOTE: We disregard errors for now
	err := n.primary.Put(c, prov, piece)
	if err != nil {
		log.Errorw("Error importing cid", "cid", err)
	} else {
		// TODO: Change to Debug
		log.Infow("Imported successfully", "cid", c)
	}
	return nil
}
