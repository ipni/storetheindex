package model

import (
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/multiformats/go-multiaddr"
)

// MakeRegisterRequest creates a signed peer.PeerRecord as a register request
// and marshals this into bytes
func MakeRegisterRequest(providerID peer.ID, privateKey crypto.PrivKey, addrs []string) ([]byte, error) {
	if len(addrs) == 0 {
		return nil, errors.New("missing address")
	}

	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, m := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(m)
		if err != nil {
			return nil, fmt.Errorf("bad address: %s", err)
		}
	}

	rec := peer.NewPeerRecord()
	rec.PeerID = providerID
	rec.Addrs = maddrs

	return makeRequestEnvelop(rec, privateKey)
}

// ReadRegisterRequest unmarshals a peer.PeerRequest from bytes, verifies the
// signature, and returns a peer.PeerRecord
func ReadRegisterRequest(data []byte) (*peer.PeerRecord, error) {
	envelope, untypedRecord, err := record.ConsumeEnvelope(data, peer.PeerRecordEnvelopeDomain)
	if err != nil {
		return nil, fmt.Errorf("cannot consume register request envelope: %s", err)
	}
	rec, ok := untypedRecord.(*peer.PeerRecord)
	if !ok {
		return nil, fmt.Errorf("unmarshaled register request record is not a *PeerRecord")
	}
	signerID, err := peer.IDFromPublicKey(envelope.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("cannot convert public key to peer ID: %w", err)
	}
	if signerID != rec.PeerID {
		return nil, errors.New("request not signed by provider")
	}

	return rec, nil
}
