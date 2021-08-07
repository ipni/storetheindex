package handler

import (
	"io/ioutil"
	"net/http"
	//"github.com/filecoin-project/storetheindex/api/v0/ingestion"
	//"github.com/ipfs/go-cid"
)

// PUT /ingestion/advertisement
func (h *Handler) Advertise(w http.ResponseWriter, r *http.Request) {
	/*
		w.Header().Set("Content-Type", "application/json")
		adBuild := ingestion.Type.Advertisement.NewBuilder()
		err := dagjson.Decoder(ptp, r.Body)
		if err != nil {
			log.Errorw("Advertise request json decode", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		_, err := h.registry.Advertise(r.Context(), adBuild.Build().(ingestion.Advertisement))
		if err != nil {
			log.Errorw("Advertise failed:", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	*/
	w.WriteHeader(http.StatusNoContent)
}

// POST /ingestion
func (h *Handler) IndexContent(w http.ResponseWriter, r *http.Request) {
	//body, err := ioutil.ReadAll(r.Body)
	_, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// TODO: decode request, auth,  and save content

	w.WriteHeader(http.StatusNoContent)
}
