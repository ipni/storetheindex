package httpserver

import (
	"io/ioutil"
	"net/http"

	"github.com/filecoin-project/storetheindex/client/models"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
)

func (s *Server) GetSingleCidHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	mhCid := vars["cid"]
	c, err := cid.Decode(mhCid)
	if err != nil {
		log.Errorw("error decoding cid", "cid", mhCid, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.getCids(w, []cid.Cid{c})
}

func (s *Server) GetBatchCidHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req, err := models.UnmarshalReq(body)
	if err != nil {
		log.Errorw("error unmarshalling body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	s.getCids(w, req.Cids)
}

func writeResponse(w http.ResponseWriter, body []byte) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if _, err := w.Write(body); err != nil {
		return err
	}

	return nil
}

func (s *Server) getCids(w http.ResponseWriter, cids []cid.Cid) {
	log.Debugw("Find cids", "cids", cids)

	resp, err := models.PopulateResp(s.engine, cids)
	if err != nil {
		log.Errorw("failed generating response", "cids", cids, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// If Cids not found
	if len(resp.Cids) == 0 {
		log.Infof("cid %v not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	rb, err := models.MarshalResp(resp)
	if err != nil {
		log.Errorw("failed marshalling response", "cid", cids, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = writeResponse(w, rb)
	if err != nil {
		log.Errorw("failed writing response", "cid", cids, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
