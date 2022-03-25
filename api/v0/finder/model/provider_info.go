package model

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ProviderData describes a provider.
type ProviderInfo struct {
	AddrInfo              peer.AddrInfo
	LastAdvertisement     cid.Cid        `json:",omitempty"`
	LastAdvertisementTime string         `json:",omitempty"`
	Publisher             *peer.AddrInfo `json:",omitempty"`
}

func MakeProviderInfo(addrInfo peer.AddrInfo, lastAd cid.Cid, lastAdTime time.Time, publisherID peer.ID, publisherAddr string) ProviderInfo {
	pinfo := ProviderInfo{
		AddrInfo:          addrInfo,
		LastAdvertisement: lastAd,
	}

	if publisherID.Validate() == nil && publisherAddr != "" {
		maddr, err := multiaddr.NewMultiaddr(publisherAddr)
		if err == nil {
			pinfo.Publisher = &peer.AddrInfo{
				ID:    publisherID,
				Addrs: []multiaddr.Multiaddr{maddr},
			}
		}
	}

	if lastAd != cid.Undef && !lastAdTime.IsZero() {
		pinfo.LastAdvertisementTime = iso8601(lastAdTime)
	}
	return pinfo
}
