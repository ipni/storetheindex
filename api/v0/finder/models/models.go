package models

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// FindRequest is the client request send by end user clients
type FindRequest struct {
	Multihashes []multihash.Multihash
}

// IndexResult aggregates all values for a single multihash.
type IndexResult struct {
	Multihash multihash.Multihash
	Values    []indexer.Value
}

// FindResponse used to answer client queries/requests
type FindResponse struct {
	IndexResults []IndexResult
	Providers    []peer.AddrInfo
	// NOTE: This feature is not enabled yet.
	// Signature []byte	// Providers signature.
}

// MarshalReq serializes the request.
// We currently JSON, we could use anything else.
//NOTE: Consider using other serialization formats?
// We could maybe use IPLD schemas instead of structs
// for requests and response so we have any codec by design.
func MarshalFindRequest(r *FindRequest) ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalFindRequest de-serializes the request.
// We currently JSON, we could use any other format.
func UnmarshalFindRequest(b []byte) (*FindRequest, error) {
	r := &FindRequest{}
	err := json.Unmarshal(b, r)
	return r, err
}

// MarshalFindResponse serializes a find response.
func MarshalFindResponse(r *FindResponse) ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalFindResponse de-serializes a find response.
func UnmarshalFindResponse(b []byte) (*FindResponse, error) {
	r := &FindResponse{}
	err := json.Unmarshal(b, r)
	return r, err
}

// PrettyPrint a response for CLI output
func (r *FindResponse) PrettyPrint() {
	for i := range r.IndexResults {
		fmt.Println("Multihash:", r.IndexResults[i].Multihash)
		fmt.Println("Values:", r.IndexResults[i].Values)
	}
}
