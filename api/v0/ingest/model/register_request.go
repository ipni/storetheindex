package model

import (
	"github.com/ipni/go-libipni/ingest/model"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.MakeRegisterRequest instead.
func MakeRegisterRequest(providerID peer.ID, privateKey crypto.PrivKey, addrs []string) ([]byte, error) {
	return model.MakeRegisterRequest(providerID, privateKey, addrs)
}

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.ReadRegisterRequest instead.
func ReadRegisterRequest(data []byte) (*peer.PeerRecord, error) {
	return model.ReadRegisterRequest(data)
}
