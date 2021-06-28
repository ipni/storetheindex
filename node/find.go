package node

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/adlrocha/indexer-node/node/models"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
)

func (n *Node) GetSingleCidHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	m := vars["cid"]
	c, err := cid.Decode(m)
	if err != nil {
		log.Errorw("error decoding cid:", m, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Find cid: ", "cid", m)
	// Get Cid from primary storage
	i, _ := n.primary.Get(c)
	out := models.FindResp{
		Providers: []models.Provider{},
	}
	// TODO: This response format is not the right one. Revise this.
	for _, k := range i {
		out.Providers = append(out.Providers,
			models.Provider{ProviderID: k.ProvID, Cids: []cid.Cid{c}})
	}

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

	if _, err := w.Write(body); err != nil {
		return err
	}

	return nil
}
