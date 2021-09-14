package models

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Request is the client request send by end user clients
type Request struct {
	Cids []cid.Cid
}

// CidResult aggregates all values for a single CID.
type CidResult struct {
	Cid    cid.Cid
	Values []indexer.Value
}

// Response used to answer client queries/requests
type Response struct {
	CidResults []CidResult
	Providers  []peer.AddrInfo
	// NOTE: This feature is not enabled yet.
	// Signature []byte	// Providers signature.
}

// MarshalReq serializes the request.
// We currently JSON, we could use anything else.
//NOTE: Consider using other serialization formats?
// We could maybe use IPLD schemas instead of structs
// for requests and response so we have any codec by design.
func MarshalReq(r *Request) ([]byte, error) {
	return json.Marshal(r)
}

// MarshalResp serializes the response.
func MarshalResp(r *Response) ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalReq de-serializes the request.
// We currently JSON, we could use any other format.
func UnmarshalReq(b []byte) (*Request, error) {
	r := &Request{}
	err := json.Unmarshal(b, r)
	return r, err
}

// UnmarshalResp de-serializes the response.
func UnmarshalResp(b []byte) (*Response, error) {
	r := &Response{}
	err := json.Unmarshal(b, r)
	return r, err
}

// PrettyPrint a response for CLI output
func (r *Response) PrettyPrint() {
	for i := range r.CidResults {
		fmt.Println("Cid:", r.CidResults[i].Cid)
		fmt.Println("Values:", r.CidResults[i].Values)
	}
}
