package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

// FindRequest is the client request send by end user clients
type FindRequest struct {
	Multihashes []multihash.Multihash
}

type ProviderResult struct {
	// ContextID identifies the metadata that is part of this value
	ContextID []byte
	// Metadata is serialized data that provides information about retrieving
	// data, for the indexed CID, from the identified provider.
	Metadata []byte `json:",omitempty"`
	// Privider is the peer ID of the provider and its multiaddrs
	Provider peer.AddrInfo
}

// MultihashResult aggregates all values for a single multihash.
type MultihashResult struct {
	Multihash       multihash.Multihash
	ProviderResults []ProviderResult
}

// FindResponse used to answer client queries/requests
type FindResponse struct {
	MultihashResults []MultihashResult
	// NOTE: This feature is not enabled yet.
	// Signature []byte	// Providers signature.
}

// Equal compares ProviderResult values to determine if they are equal.  The
// provider addresses are omitted from the comparison.
func (pr ProviderResult) Equal(other ProviderResult) bool {
	if !bytes.Equal(pr.ContextID, other.ContextID) {
		return false
	}
	if !bytes.Equal(pr.Metadata, other.Metadata) {
		return false
	}
	if pr.Provider.ID != other.Provider.ID {
		return false
	}
	return true
}

func ProviderResultFromValue(value indexer.Value, addrs []multiaddr.Multiaddr) ProviderResult {
	return ProviderResult{
		ContextID: value.ContextID,
		Metadata:  value.Metadata,
		Provider: peer.AddrInfo{
			ID:    value.ProviderID,
			Addrs: addrs,
		},
	}
}

// MarshalReq serializes the request. Currently uses JSON, but could use
// anything else.
//
// NOTE: Consider using other serialization formats?  We could maybe use IPLD
// schemas instead of structs for requests and response so we have any codec by
// design.
func MarshalFindRequest(r *FindRequest) ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalFindRequest de-serializes the request.
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

func (r *FindResponse) String() string {
	var b strings.Builder
	for i := range r.MultihashResults {
		data, err := json.MarshalIndent(&r.MultihashResults[i], "", "  ")
		if err != nil {
			return err.Error()
		}
		b.Write(data)
		b.WriteByte(0x0a)
	}
	return b.String()
}

// PrettyPrint a response for CLI output
func (r *FindResponse) PrettyPrint() {
	fmt.Println(r.String())
}
