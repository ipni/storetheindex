package net_test

import (
	"testing"

	"github.com/filecoin-project/storetheindex/server/net"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestP2PEndpoint(t *testing.T) {
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	end, err := net.NewP2PEndpoint("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	if err != nil {
		t.Fatal(err)
	}
	if end.Addr() != p {
		t.Fatal("wrong endpoint")
	}

	// This is a wrong peer.ID. We shouldn't generate endpoints for wrong peer.IDs.
	_, err = net.NewP2PEndpoint("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1Bcv")
	if err == nil {
		t.Fatal("shouldn't have generated endpoint")
	}

}
