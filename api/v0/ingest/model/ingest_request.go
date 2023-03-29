package model

import (
	"github.com/ipni/go-libipni/ingest/model"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.IngestRequest instead.
type IngestRequest = model.IngestRequest

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.IngestRequestEnvelopeDomain instead.
const IngestRequestEnvelopeDomain = model.IngestRequestEnvelopeDomain

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.IngestRequestEnvelopePayloadType instead.
var IngestRequestEnvelopePayloadType = model.IngestRequestEnvelopePayloadType

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.MakeIngestRequest instead.
func MakeIngestRequest(providerID peer.ID, privateKey crypto.PrivKey, m multihash.Multihash, contextID []byte, metadata []byte, addrs []string) ([]byte, error) {
	return model.MakeIngestRequest(providerID, privateKey, m, contextID, metadata, addrs)
}

// Deprecated: Use github.com/ipni/go-libipni/ingest/model.ReadIngestRequest instead.
func ReadIngestRequest(data []byte) (*IngestRequest, error) {
	return model.ReadIngestRequest(data)
}
