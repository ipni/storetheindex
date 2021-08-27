package models

import (
	"bytes"
	"fmt"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/signature"
	"github.com/ipfs/go-cid"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type DiscoverRequest struct {
	ProviderID    peer.ID
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
	Value     indexer.Value
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

func (r *DiscoverRequest) Sign(privKey ic.PrivKey) error {
	var err error
	r.Nonce, err = signature.Nonce()
	if err != nil {
		return err
	}
	r.Signature, err = privKey.Sign(r.signedData())
	return err
}

func (r *DiscoverRequest) VerifySignature() error {
	return signature.Verify(r.ProviderID, r.signedData(), r.Signature)
}

func (r *DiscoverRequest) signedData() []byte {
	buf := bytes.NewBuffer(make([]byte, len(r.DiscoveryAddr)+len(r.Nonce)))
	buf.WriteString(r.DiscoveryAddr)
	buf.Write(r.Nonce)
	return buf.Bytes()
}

func (r *RegisterRequest) Sign(privKey ic.PrivKey) error {
	var err error
	r.Nonce, err = signature.Nonce()
	if err != nil {
		return err
	}
	r.Signature, err = privKey.Sign(r.signedData())
	return err
}

func (r *RegisterRequest) VerifySignature() error {
	return signature.Verify(r.AddrInfo.ID, r.signedData(), r.Signature)
}

func (r *RegisterRequest) signedData() []byte {
	var buf bytes.Buffer
	for _, a := range r.AddrInfo.Addrs {
		buf.Write(a.Bytes())
	}
	buf.Write(r.Nonce)
	return buf.Bytes()
}

func (r *IngestRequest) Sign(privKey ic.PrivKey) error {
	var err error
	r.Nonce, err = signature.Nonce()
	if err != nil {
		return err
	}
	r.Signature, err = privKey.Sign(r.signedData())
	return err
}

func (r *IngestRequest) VerifySignature() error {
	return signature.Verify(r.Value.ProviderID, r.signedData(), r.Signature)
}

func (r *IngestRequest) signedData() []byte {
	buf := bytes.NewBuffer(make([]byte, r.Cid.ByteLen()+len(r.Value.Metadata)+len(r.Nonce)))
	buf.Write(r.Cid.Bytes())
	buf.Write(r.Value.Metadata)
	buf.Write(r.Nonce)
	return buf.Bytes()
}

// MakeRegisterRequest creates a signed RegisterRequest
func MakeRegisterRequest(providerIdent config.Identity, addrs []string) (*RegisterRequest, error) {
	providerID, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		return nil, fmt.Errorf("could not decode peer id: %s", err)
	}

	privKey, err := providerIdent.DecodePrivateKey("")
	if err != nil {
		return nil, fmt.Errorf("could not decode private key: %s", err)
	}

	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, m := range addrs {
		maddrs[i], err = multiaddr.NewMultiaddr(m)
		if err != nil {
			return nil, fmt.Errorf("bad provider address: %s", err)
		}
	}

	regReq := &RegisterRequest{
		AddrInfo: peer.AddrInfo{
			ID:    providerID,
			Addrs: maddrs,
		},
	}

	if err = regReq.Sign(privKey); err != nil {
		return nil, fmt.Errorf("cannot sign request: %s", err)
	}

	return regReq, nil
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

// iso8601 returns the given time as an ISO8601 formatted string.
func iso8601(t time.Time) string {
	tstr := t.Format("2006-01-02T15:04:05")
	_, zoneOffset := t.Zone()
	if zoneOffset == 0 {
		return fmt.Sprintf("%sZ", tstr)
	}
	if zoneOffset < 0 {
		return fmt.Sprintf("%s-%02d%02d", tstr, -zoneOffset/3600,
			(-zoneOffset%3600)/60)
	}
	return fmt.Sprintf("%s+%02d%02d", tstr, zoneOffset/3600,
		(zoneOffset%3600)/60)
}
