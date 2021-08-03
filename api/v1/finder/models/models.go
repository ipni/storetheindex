package models

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// MultiaddrSlice is a list of muliaddr.
type MultiaddrSlice []ma.Multiaddr

// UnmarshalJSON for MultiaddrSlice
func (m *MultiaddrSlice) UnmarshalJSON(raw []byte) (err error) {
	var temp []string
	if err := json.Unmarshal(raw, &temp); err != nil {
		return err
	}

	res := make([]ma.Multiaddr, len(temp))
	for i, str := range temp {
		res[i], err = ma.NewMultiaddr(str)
		if err != nil {
			return err
		}
	}
	*m = res
	return nil
}

// Request is the client request send by end user clients
type Request struct {
	Cids []cid.Cid
}

// ProviderData aggregates provider-related data that wants to
// be added in a response
type ProviderData struct {
	Provider peer.ID
	Addrs    MultiaddrSlice
}

// CidData aggregates all entries for a single CID.
type CidData struct {
	Cid     cid.Cid
	Entries []entry.Value
}

// Response used to answer client queries/requests
type Response struct {
	Cids      []CidData
	Providers []ProviderData
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

// PopulateResp reads from indexer engine to populate a response from a list of CIDs.
func PopulateResp(e *indexer.Engine, cids []cid.Cid) (*Response, error) {
	out := Response{
		Cids:      make([]CidData, len(cids)),
		Providers: make([]ProviderData, 0),
	}
	num := 0

	for i := range cids {
		v, f, err := e.Get(cids[i])
		if err != nil {
			// If error and the request is not for a batch, continue without
			// sending an error as if the errored CID wasn't found
			if len(cids) < 1 {
				return nil, err
			}
			continue
		}
		// If found add to response
		if f {
			out.Cids[i] = CidData{Cid: cids[i], Entries: v}
			num++
		}
		// TODO: Add multiaddr of the providers included in the entry
		// if no information has been added yet. This feature is not
		// supported yet, we haven't decided how to (and from where) to
		// get this data.
	}

	out.Cids = out.Cids[:num]
	return &out, nil
}

// PrettyPrint a response for CLI output
func (r *Response) PrettyPrint() {
	for i := range r.Cids {
		fmt.Println("Cid:", r.Cids[i].Cid)
		fmt.Println("Entries:", r.Cids[i].Entries)
	}
}
