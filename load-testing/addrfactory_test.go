package main

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
)

func TestAddrFactory(t *testing.T) {
	mapping := map[string]string{
		"127.0.0.1": "127.0.0.2",
	}

	addrFactory := NewAddrsFactory(mapping)
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9001")
	if addrFactory([]multiaddr.Multiaddr{ma})[0].String() != "/ip4/127.0.0.2/tcp/9001" {
		t.Fatalf("Failed address translation")
	}
}
