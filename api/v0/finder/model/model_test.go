package model

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

const testProtoID = 0x300000

var rng = rand.New(rand.NewSource(1413))

func TestMarshal(t *testing.T) {
	// Generate some multihashes and populate indexer
	mhs := util.RandomMultihashes(3, rng)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	ctxID := []byte("test-context-id")
	metadata := v0.Metadata{
		ProtocolID: testProtoID,
		Data:       []byte(mhs[0]),
	}

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
	if !equalMultihashes(r.Multihashes, mhs) {
		t.Fatal("Request marshal/unmarshal not correct")
	}

	m1, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	if err != nil {
		t.Fatal(err)
	}

	// Masrhal response and check e2e
	t.Log("e2e marshalling response")
	resp := &FindResponse{
		MultihashResults: make([]MultihashResult, 0),
	}

	providerResult := ProviderResult{
		ContextID: ctxID,
		Metadata:  metadata,
		Provider: peer.AddrInfo{
			ID:    p,
			Addrs: []multiaddr.Multiaddr{m1},
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
	if !equalMultihashResult(resp.MultihashResults, r2.MultihashResults) {
		t.Fatal("failed marshal/unmarshaling response")
	}

}

func equalMultihashResult(res1, res2 []MultihashResult) bool {
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

func equalMultihashes(m1, m2 []multihash.Multihash) bool {
	if len(m1) != len(m2) {
		return false
	}
	for i := range m1 {
		if !bytes.Equal([]byte(m1[i]), []byte(m2[i])) {
			return false
		}
	}
	return true
}
