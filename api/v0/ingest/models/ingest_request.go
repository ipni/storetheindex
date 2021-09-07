package models

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
)

// IgenstRequest is a request to store a single CID.  This is intentionally
// limited to one CID as bulk CID ingestion should be done via advertisement
// ingestion method.
type IngestRequest struct {
	Cid   cid.Cid
	Value indexer.Value
	Seq   uint64
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
func MakeIngestRequest(providerIdent config.Identity, c cid.Cid, protocol uint64, metadata []byte) ([]byte, error) {
	providerID, privKey, err := providerIdent.Decode()
	if err != nil {
		return nil, err
	}

	value := indexer.MakeValue(providerID, protocol, metadata)

	req := &IngestRequest{
		Cid:   c,
		Value: value,
		Seq:   peer.TimestampSeq(),
	}

	return makeRequestEnvelop(req, privKey)
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
