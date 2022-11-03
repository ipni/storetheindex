package model

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ProviderData describes a provider.
type ProviderInfo struct {
	AddrInfo              peer.AddrInfo
	LastAdvertisement     cid.Cid        `json:",omitempty"`
	LastAdvertisementTime string         `json:",omitempty"`
	Publisher             *peer.AddrInfo `json:",omitempty"`
	IndexCount            uint64         `json:",omitempty"`
}

func MakeProviderInfo(addrInfo peer.AddrInfo, lastAd cid.Cid, lastAdTime time.Time, publisherID peer.ID, publisherAddr multiaddr.Multiaddr, indexCount uint64) ProviderInfo {
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
	return pinfo
}
