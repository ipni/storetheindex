package model

import (
	"time"

	"github.com/ipfs/go-cid"
	_ "github.com/ipni/storetheindex/dagsync/httpsync/maconv"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ProviderData describes a provider.
type ProviderInfo struct {
	AddrInfo              peer.AddrInfo
	LastAdvertisement     cid.Cid            `json:",omitempty"`
	LastAdvertisementTime string             `json:",omitempty"`
	Publisher             *peer.AddrInfo     `json:",omitempty"`
	IndexCount            uint64             `json:",omitempty"`
	ExtendedProviders     *ExtendedProviders `json:",omitempty"`
	FrozenAt              cid.Cid            `json:",omitempty"`
	FrozenAtTime          string             `json:",omitempty"`
}

type ExtendedProviders struct {
	Providers  []peer.AddrInfo               `json:",omitempty"`
	Contextual []ContextualExtendedProviders `json:",omitempty"`
}

type ContextualExtendedProviders struct {
	Override  bool
	ContextID string
	Providers []peer.AddrInfo
}

func MakeProviderInfo(addrInfo peer.AddrInfo, lastAd cid.Cid, lastAdTime time.Time, publisherID peer.ID, publisherAddr multiaddr.Multiaddr, frozenAt cid.Cid, frozenAtTime time.Time, indexCount uint64) ProviderInfo {
	pinfo := ProviderInfo{
		AddrInfo:          addrInfo,
		LastAdvertisement: lastAd,
		IndexCount:        indexCount,
	}

	if publisherID.Validate() == nil && publisherAddr != nil {
		pinfo.Publisher = &peer.AddrInfo{
			ID:    publisherID,
			Addrs: []multiaddr.Multiaddr{publisherAddr},
		}
	}

	if lastAd != cid.Undef && !lastAdTime.IsZero() {
		pinfo.LastAdvertisementTime = iso8601(lastAdTime)
	}

	if frozenAt != cid.Undef {
		pinfo.FrozenAt = frozenAt
	}
	if !frozenAtTime.IsZero() {
		pinfo.FrozenAtTime = iso8601(frozenAtTime)
	}

	return pinfo
}
