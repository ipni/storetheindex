package models

import (
	"encoding/json"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// MultiaddrSlice is a list of muliaddr.
type MultiaddrSlice []ma.Multiaddr

// UnmarshalJSON for MultiaddrSlice
func (m *MultiaddrSlice) UnmarshalJSON(raw []byte) (err error) {
	var temp []string
	if err := json.Unmarshal(raw, &temp); err != nil {
		return err
	}

	res := make([]ma.Multiaddr, len(temp))
	for i, str := range temp {
		res[i], err = ma.NewMultiaddr(str)
		if err != nil {
			return err
		}
	}
	*m = res
	return nil
}

// ProviderData aggregates provider-related data that wants to
// be added in a response
type ProviderData struct {
	Provider  peer.ID
	Addrs     MultiaddrSlice
	LastIndex cid.Cid `json:"omitempty"`
}
