package models

import (
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type FindResp struct {
	Providers []Provider `json:"providers"`
}

type Provider struct {
	ProviderID peer.ID   `json:"provider_id"`
	Cids       []cid.Cid `json:"cids"`
}
