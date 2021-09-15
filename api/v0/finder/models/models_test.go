package models

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/filecoin-project/storetheindex/internal/utils"
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
	v := indexer.MakeValue(p, 0, []byte(mhs[0]))

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
	if !utils.EqualMultihashes(r.Multihashes, mhs) {
		t.Fatal("Request marshal/unmarshal not correct")
	}

	// Masrhal response and check e2e
	t.Log("e2e marshalling response")
	resp := &FindResponse{
		MultihashResults: make([]MultihashResult, 0),
		Providers:        make([]peer.AddrInfo, 0),
	}

	for i := range mhs {
		resp.MultihashResults = append(resp.MultihashResults, MultihashResult{mhs[i], []indexer.Value{v}})
	}
	m1, err := ma.NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	if err != nil {
		t.Fatal(err)
	}

	resp.Providers = append(resp.Providers, peer.AddrInfo{ID: p, Addrs: []ma.Multiaddr{m1}})

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
	for i := range res1 {
		if !bytes.Equal([]byte(res1[i].Multihash), []byte(res2[i].Multihash)) || !utils.EqualValues(res1[i].Values, res2[i].Values) {
			return false
		}
	}
	return true
}
