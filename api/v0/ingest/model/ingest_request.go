package model

import (
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/multiformats/go-multihash"
)

// IgenstRequest is a request to store a single multihash.  This is
// intentionally limited to one multihash as bulk ingestion should be done via
// advertisement ingestion method.
type IngestRequest struct {
	Multihash  multihash.Multihash
	ProviderID peer.ID
	ContextID  []byte
	Metadata   []byte
	Addrs      []string
	Seq        uint64
}

// IngestRequestEnvelopeDomain is the domain string used for ingest requests contained in a Envelope.
const IngestRequestEnvelopeDomain = "indexer-ingest-request-record"

// IngestRequestEnvelopePayloadType is the type hint used to identify IngestRequest records in a Envelope.
var IngestRequestEnvelopePayloadType = []byte("indexer-ingest-request")

func init() {
	record.RegisterType(&IngestRequest{})
}

// Domain is used when signing and validating IngestRequest records contained in Envelopes
func (r *IngestRequest) Domain() string {
	return IngestRequestEnvelopeDomain
}

// Codec is a binary identifier for the IngestRequest type
func (r *IngestRequest) Codec() []byte {
	return IngestRequestEnvelopePayloadType
}

// UnmarshalRecord parses an IngestRequest from a byte slice.
func (r *IngestRequest) UnmarshalRecord(data []byte) error {
	if r == nil {
		return fmt.Errorf("cannot unmarshal IngestRequest to nil receiver")
	}

	return json.Unmarshal(data, r)
}

// MarshalRecord serializes an IngestRequesr to a byte slice.
func (r *IngestRequest) MarshalRecord() ([]byte, error) {
	return json.Marshal(r)
}

// MakeIngestRequest creates a signed IngestRequest and marshals it into bytes
func MakeIngestRequest(providerID peer.ID, privateKey crypto.PrivKey, m multihash.Multihash, contextID []byte, metadata []byte, addrs []string) ([]byte, error) {
	req := &IngestRequest{
		Multihash:  m,
		ProviderID: providerID,
		ContextID:  contextID,
		Metadata:   metadata,
		Addrs:      addrs,
		Seq:        peer.TimestampSeq(),
	}

	return makeRequestEnvelop(req, privateKey)
}

// ReadIngestRequest unmarshals an IngestRequest from bytes, verifies the
// signature, and returns the IngestRequest
func ReadIngestRequest(data []byte) (*IngestRequest, error) {
	_, untypedRecord, err := record.ConsumeEnvelope(data, IngestRequestEnvelopeDomain)
	if err != nil {
		return nil, fmt.Errorf("cannot consume register request envelope: %s", err)
	}
	rec, ok := untypedRecord.(*IngestRequest)
	if !ok {
		return nil, fmt.Errorf("unmarshaled request is not a *IngestRequest")
	}
	return rec, nil
}
