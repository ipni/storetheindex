package model

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMarshal(t *testing.T) {
	// Generate some multihashes and populate indexer
	mhs, err := test.RandomMultihashes(3)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	ctxID := []byte("test-context-id")
	v := indexer.MakeValue(p, ctxID, 0, []byte(mhs[0]))

	// Masrhal request and check e2e
	t.Log("e2e marshalling request")
	req := &FindRequest{Multihashes: mhs}
	b, err := MarshalFindRequest(req)
	if err != nil {
		t.Fatal(err)
	}

	r, err := UnmarshalFindRequest(b)
	if err != nil {
		t.Fatal(err)
	}
	if !util.EqualMultihashes(r.Multihashes, mhs) {
		t.Fatal("Request marshal/unmarshal not correct")
	}

	m1, err := ma.NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	if err != nil {
		t.Fatal(err)
	}

	// Masrhal response and check e2e
	t.Log("e2e marshalling response")
	resp := &FindResponse{
		MultihashResults: make([]MultihashResult, 0),
	}

	providerResult := ProviderResult{
		ContextID: v.ContextID,
		Metadata:  v.Metadata,
		Provider: peer.AddrInfo{
			ID:    p,
			Addrs: []ma.Multiaddr{m1},
		},
	}

	for i := range mhs {
		resp.MultihashResults = append(resp.MultihashResults, MultihashResult{
			Multihash:       mhs[i],
			ProviderResults: []ProviderResult{providerResult},
		})
	}

	b, err = MarshalFindResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	r2, err := UnmarshalFindResponse(b)
	if err != nil {
		t.Fatal(err)
	}
	if !EqualMultihashResult(resp.MultihashResults, r2.MultihashResults) {
		t.Fatal("failed marshal/unmarshaling response")
	}

}

func EqualMultihashResult(res1, res2 []MultihashResult) bool {
	if len(res1) != len(res2) {
		return false
	}
	for i, r1 := range res1 {
		r2 := res2[i]
		if !bytes.Equal([]byte(r1.Multihash), []byte(r2.Multihash)) {
			return false
		}
		if len(r1.ProviderResults) != len(r2.ProviderResults) {
			return false
		}
		for j, pr1 := range r1.ProviderResults {
			if !pr1.Equal(r2.ProviderResults[j]) {
				return false
			}
		}
	}
	return true
}
