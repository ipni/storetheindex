package models

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type DiscoverRequest struct {
	DiscoveryAddr string
	Nonce         []byte
	Signature     []byte
}

type RegisterRequest struct {
	AddrInfo  peer.AddrInfo
	Nonce     []byte
	Signature []byte
}

// IgenstRequest is a request to store a single CID.  This is intentionally
// limited to one CID as bulk CID ingestion should be done via advertisement
// ingestion method.
type IngestRequest struct {
	Cid       cid.Cid
	Provider  peer.ID
	Protocol  int
	Metadata  []byte
	Nonce     []byte
	Signature []byte
}

// ProviderData aggregates provider-related data that wants to
// be added in a response
type ProviderInfo struct {
	AddrInfo      peer.AddrInfo
	LastIndex     cid.Cid
	LastIndexTime string `json:"omitempty"`
}
