package models

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/syserr"
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

// PopulateResponse reads from indexer core to populate a response from a
// list of CIDs.
//
// TODO: This should be relocated to a place common to all finder handlers, but
// not under /api/
func PopulateResponse(indexer indexer.Interface, registry *providers.Registry, cids []cid.Cid) (*Response, error) {
	cidResults := make([]CidResult, 0, len(cids))
	var providerResults []peer.AddrInfo
	providerSeen := map[peer.ID]struct{}{}

	for i := range cids {
		values, found, err := indexer.Get(cids[i])
		if err != nil {
			return nil, syserr.New(fmt.Errorf("failed to query cid %q: %s", cids[i], err), 500)
		}
		if !found {
			continue
		}
		// Add the response to the list of CID responses
		cidResults = append(cidResults, CidResult{Cid: cids[i], Values: values})

		// Lookup provider info for each unique provider
		for j := range values {
			provID := values[j].ProviderID
			if _, found = providerSeen[provID]; found {
				continue
			}
			providerSeen[provID] = struct{}{}

			pinfo := registry.ProviderInfo(provID)
			if pinfo == nil {
				//log.Errorw("no info for provider", "provider_id", provID)
				continue
			}

			providerResults = append(providerResults, pinfo.AddrInfo)
		}
	}

	return &Response{
		CidResults: cidResults,
		Providers:  providerResults,
	}, nil
}

// PrettyPrint a response for CLI output
func (r *Response) PrettyPrint() {
	for i := range r.CidResults {
		fmt.Println("Cid:", r.CidResults[i].Cid)
		fmt.Println("Values:", r.CidResults[i].Values)
	}
}
