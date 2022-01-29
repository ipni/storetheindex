package model

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// ProviderData describes a provider.
type ProviderInfo struct {
	AddrInfo              peer.AddrInfo
	LastAdvertisement     cid.Cid `json:",omitempty"`
	LastAdvertisementTime string  `json:",omitempty"`
}

func MakeProviderInfo(addrInfo peer.AddrInfo, lastAd cid.Cid, lastAdTime time.Time) ProviderInfo {
	pinfo := ProviderInfo{
		AddrInfo:          addrInfo,
		LastAdvertisement: lastAd,
	}

	if lastAd != cid.Undef && !lastAdTime.IsZero() {
		pinfo.LastAdvertisementTime = iso8601(lastAdTime)
	}
	return pinfo
}
