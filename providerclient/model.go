package client

import (
	"bytes"
	"encoding/json"

	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
)

// AdRequest requests for a specific Advertisement by CID
type AdRequest struct {
	ID cid.Cid
}

// AdResponse with the advertisement for a CID.
type AdResponse struct {
	ID cid.Cid
	Ad schema.Advertisement
}

// Auxiliary struct used to encapsulate advertisement encoding.
type wrap struct {
	ID cid.Cid
	Ad []byte
}

// MarshalAdRequest serializes the request.
func MarshalAdRequest(r *AdRequest) ([]byte, error) {
	return json.Marshal(r)
}

// MarshalAdResponse serializes the response.
func MarshalAdResponse(r *AdResponse) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := dagjson.Encode(r.Ad.Representation(), buf)
	if err != nil {
		return nil, err
	}
	w := &wrap{ID: r.ID, Ad: buf.Bytes()}
	return json.Marshal(w)
}

// UnmarshalAdRequest de-serializes the request.
// We currently JSON, we could use any other format.
func UnmarshalAdRequest(b []byte) (*AdRequest, error) {
	r := &AdRequest{}
	err := json.Unmarshal(b, r)
	return r, err
}

// UnmarshalAdResponse de-serializes the response.
func UnmarshalAdResponse(b []byte) (*AdResponse, error) {
	w := &wrap{}
	err := json.Unmarshal(b, w)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(w.Ad)
	nb := schema.Type.Advertisement.NewBuilder()
	err = dagjson.Decode(nb, buf)
	if err != nil {
		return nil, err
	}
	return &AdResponse{ID: w.ID, Ad: nb.Build().(schema.Advertisement)}, err
}
