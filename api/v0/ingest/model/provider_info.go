package model

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// ProviderData aggregates provider-related data that wants to
// be added in a response
type ProviderInfo struct {
	AddrInfo      peer.AddrInfo
	LastIndex     cid.Cid `json:",omitempty"`
	LastIndexTime string  `json:",omitempty"`
}

func MakeProviderInfo(addrInfo peer.AddrInfo, lastIndex cid.Cid, lastIndexTime time.Time) ProviderInfo {
	pinfo := ProviderInfo{
		AddrInfo:  addrInfo,
		LastIndex: lastIndex,
	}

	if lastIndex.Defined() {
		pinfo.LastIndexTime = iso8601(lastIndexTime)
	}
	return pinfo
}
