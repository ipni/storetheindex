package node

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
)

func (n *Node) GetSingleCidHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	mhCid := vars["cid"]
	c, err := cid.Decode(mhCid)
	if err != nil {
		log.Errorw("error decoding cid", "cid", mhCid, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Find cid", "cid", mhCid)
	// Get Cid from primary storage
	i, _, _ := n.primary.Get(c)
	out := map[cid.Cid][]store.IndexEntry{c: i}

	err = writeResponse(w, out)
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// w.WriteHeader(http.StatusOK)

}

func writeResponse(w http.ResponseWriter, r interface{}) error {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if _, err = w.Write(body); err != nil {
		return err
	}

	return nil
}
