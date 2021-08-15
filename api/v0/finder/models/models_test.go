package models

import (
	"testing"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/go-indexer-core/store/test"
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMarshal(t *testing.T) {

	// Generate some CIDs and populate indexer
	cids, err := test.RandomCids(3)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	e := entry.MakeValue(p, 0, cids[0].Bytes())

	// Masrhal request and check e2e
	t.Log("e2e marshalling request")
	req := &Request{Cids: cids}
	b, err := MarshalReq(req)
	if err != nil {
		t.Fatal(err)
	}

	r, err := UnmarshalReq(b)
	if err != nil {
		t.Fatal(err)
	}
	if !utils.EqualCids(r.Cids, cids) {
		t.Fatal("Request marshal/unmarshal not correct")
	}

	// Masrhal response and check e2e
	t.Log("e2e marshalling response")
	resp := &Response{
		CidResults: make([]CidResult, 0),
		Providers:  make([]peer.AddrInfo, 0),
	}

	for i := range cids {
		resp.CidResults = append(resp.CidResults, CidResult{cids[i], []entry.Value{e}})
	}
	m1, err := ma.NewMultiaddr("/ip4/127.0.0.1/udp/1234")
	if err != nil {
		t.Fatal(err)
	}

	resp.Providers = append(resp.Providers, peer.AddrInfo{ID: p, Addrs: []ma.Multiaddr{m1}})

	b, err = MarshalResp(resp)
	if err != nil {
		t.Fatal(err)
	}

	r2, err := UnmarshalResp(b)
	if err != nil {
		t.Fatal(err)
	}
	if !EqualCidResult(resp.CidResults, r2.CidResults) {
		t.Fatal("failed marshal/unmarshaling response")
	}

}

func EqualCidResult(e1, e2 []CidResult) bool {
	if len(e1) != len(e2) {
		return false
	}
	for i := range e1 {
		if e1[i].Cid == e2[i].Cid && !utils.EqualEntries(e1[i].Entries, e2[i].Entries) {
			return false
		}
	}
	return true
}
