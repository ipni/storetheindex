package models

import (
	"fmt"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
	"github.com/multiformats/go-multiaddr"
)

// MakeRegisterRequest creates a signed peer.PeerRecord as a register request
// and marshals this into bytes
func MakeRegisterRequest(providerID, privateKey string, addrs []string) ([]byte, error) {
	providerIdent := config.Identity{
		PeerID:  providerID,
		PrivKey: privateKey,
	}
	peerID, privKey, err := providerIdent.Decode()
	if err != nil {
		return nil, err
	}

	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, m := range addrs {
		maddrs[i], err = multiaddr.NewMultiaddr(m)
		if err != nil {
			return nil, fmt.Errorf("bad provider address: %s", err)
		}
	}

	rec := peer.NewPeerRecord()
	rec.PeerID = peerID
	rec.Addrs = maddrs

	return makeRequestEnvelop(rec, privKey)
}

// ReadRegisterRequest unmarshals a peer.PeerRequest from bytes, verifies the
// signature, and returns a peer.PeerRecord
func ReadRegisterRequest(data []byte) (*peer.PeerRecord, error) {
	_, untypedRecord, err := record.ConsumeEnvelope(data, peer.PeerRecordEnvelopeDomain)
	if err != nil {
		return nil, fmt.Errorf("cannot consume register request envelope: %s", err)
	}
	rec, ok := untypedRecord.(*peer.PeerRecord)
	if !ok {
		return nil, fmt.Errorf("unmarshaled register request record is not a *PeerRecord")
	}
	return rec, nil
}
