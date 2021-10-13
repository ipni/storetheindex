package v0

import (
	"bytes"
	"encoding"
	"fmt"

	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

type ErrInvalidMetadata struct {
	Message string
}

func (e ErrInvalidMetadata) Error() string {
	return fmt.Sprintf("storetheindex: invalid metadata: %v", e.Message)
}

// Metadata is data that provides information about retrieving
// data for an index, from a particular provider.
type Metadata struct {
	// ProtocolID defines the protocol used for data retrieval.
	ProtocolID multicodec.Code
	// Data is specific to the identified protocol, and provides data, or a
	// link to data, necessary for retrieval.
	Data []byte
}

var (
	_ encoding.BinaryMarshaler   = (*Metadata)(nil)
	_ encoding.BinaryUnmarshaler = (*Metadata)(nil)
)

// Equal determines if two Metadata values are equal.
func (m Metadata) Equal(other Metadata) bool {
	return m.ProtocolID == other.ProtocolID && bytes.Equal(m.Data, other.Data)
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (m Metadata) MarshalBinary() ([]byte, error) {
	if m.ProtocolID == 0 {
		return nil, &ErrInvalidMetadata{Message: "encountered protocol ID 0 on encode"}
	}

	varintSize := varint.UvarintSize(uint64(m.ProtocolID))
	buf := make([]byte, varintSize+len(m.Data))
	varint.PutUvarint(buf, uint64(m.ProtocolID))
	if len(m.Data) != 0 {
		copy(buf[varintSize:], m.Data)
	}
	return buf, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (m *Metadata) UnmarshalBinary(data []byte) error {
	protocol, protoLen, err := varint.FromUvarint(data)
	if err != nil {
		return err
	}
	if protocol == 0 {
		return &ErrInvalidMetadata{Message: "encountered protocol ID 0 on decode"}
	}

	m.ProtocolID = multicodec.Code(protocol)

	// We can't hold onto the input data. Make a copy.
	innerData := data[protoLen:]
	m.Data = make([]byte, len(innerData))
	copy(m.Data, innerData)

	return nil
}
