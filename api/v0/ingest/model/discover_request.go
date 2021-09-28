package model

import (
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
)

type DiscoverRequest struct {
	ProviderID    peer.ID
	DiscoveryAddr string
	Seq           uint64
}

// DiscoverRequestEnvelopeDomain is the domain string used for discover requests contained in a Envelope
const DiscoverRequestEnvelopeDomain = "indexer-discover-request-record"

// DiscoverRequestEnvelopePayloadType is the type hint used to identify DiscoverRequest records in a Envelope.
var DiscoverRequestEnvelopePayloadType = []byte("indexer-discover-request")

func init() {
	record.RegisterType(&DiscoverRequest{})
}

// Domain is used when signing and validating DiscoverRequest records contained in Envelopes
func (r *DiscoverRequest) Domain() string {
	return DiscoverRequestEnvelopeDomain
}

// Codec is a binary identifier for the DiscoverRequest type
func (r *DiscoverRequest) Codec() []byte {
	return DiscoverRequestEnvelopePayloadType
}

// UnmarshalRecord parses a DiscoverRequest from a byte slice
func (r *DiscoverRequest) UnmarshalRecord(data []byte) error {
	if r == nil {
		return fmt.Errorf("cannot unmarshal DiscoverRequest to nil receiver")
	}

	return json.Unmarshal(data, r)
}

// MarshalRecord serializes an DiscoverRequesr to a byte slice.
func (r *DiscoverRequest) MarshalRecord() ([]byte, error) {
	return json.Marshal(r)
}

// MakeDiscoverRequest creates a signed DiscoverRequest and marshals it into
// bytes
func MakeDiscoverRequest(providerID peer.ID, privateKey crypto.PrivKey, discoveryAddr string) ([]byte, error) {
	req := &DiscoverRequest{
		ProviderID:    providerID,
		DiscoveryAddr: discoveryAddr,
		Seq:           peer.TimestampSeq(),
	}

	return makeRequestEnvelop(req, privateKey)
}

// ReadDiscoverRequest unmarshals a DiscoverRequest from bytes, verifies the
// signature, and returns the DiscoverRequest
func ReadDiscoverRequest(data []byte) (*DiscoverRequest, error) {
	_, untypedRecord, err := record.ConsumeEnvelope(data, DiscoverRequestEnvelopeDomain)
	if err != nil {
		return nil, fmt.Errorf("cannot consume discover request envelope: %s", err)
	}
	rec, ok := untypedRecord.(*DiscoverRequest)
	if !ok {
		return nil, fmt.Errorf("unmarshaled request is not a *DiscoverRequest")
	}
	return rec, nil
}
