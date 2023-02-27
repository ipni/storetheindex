package model

import (
	"github.com/ipfs/go-cid"
	_ "github.com/ipni/storetheindex/dagsync/httpsync/maconv"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ProviderData describes a provider.
type ProviderInfo struct {
	AddrInfo              peer.AddrInfo
	LastAdvertisement     cid.Cid            `json:",omitempty"`
	LastAdvertisementTime string             `json:",omitempty"`
	Lag                   int                `json:",omitempty"`
	Publisher             *peer.AddrInfo     `json:",omitempty"`
	IndexCount            uint64             `json:",omitempty"`
	ExtendedProviders     *ExtendedProviders `json:",omitempty"`
	FrozenAt              cid.Cid            `json:",omitempty"`
	FrozenAtTime          string             `json:",omitempty"`
	// Inactive means that no update has been received for the configured
	// PollPeriod, and the publisher has is not responding to polls.
	Inactive bool `json:",omitempty"`
}

type ExtendedProviders struct {
	Providers  []peer.AddrInfo               `json:",omitempty"`
	Contextual []ContextualExtendedProviders `json:",omitempty"`
	Metadatas  [][]byte                      `json:",omitempty"`
}

type ContextualExtendedProviders struct {
	Override  bool
	ContextID string
	Providers []peer.AddrInfo
	Metadatas [][]byte `json:",omitempty"`
}
