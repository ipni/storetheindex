package handle

import (
	"fmt"
	"net/http"

	"github.com/adlrocha/indexer-node/importer"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("api")

// NOTE: Considering sending a JSON in the body of these handlers
// to give additional input about the error to the client.

func (a *API) ImportManifestHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: This code is the same for all import handlers.
	// We probably can take it out to its own function to deduplicate.
	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.IDFromString(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID")
		w.WriteHeader(http.StatusBadRequest)
	}
	log.Infow("Import manifest for miner: ", m)
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
			// TODO: Put in memory
			fmt.Println(c)
		case e := <-d:
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

func ImportCidListHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	m := vars["minerid"]
	miner, err := peer.IDFromString(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID")
		w.WriteHeader(http.StatusBadRequest)
	}
	log.Infow("Import manifest for miner: ", m)
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
			// TODO: Put in memory
			fmt.Println(c)
		case e := <-d:
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
