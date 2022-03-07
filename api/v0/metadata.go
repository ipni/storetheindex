package v0

import (
	"bytes"
	"encoding"
	"fmt"
	"sort"

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
	Protocols []ProtocolMetadata
}

type ProtocolMetadata interface {
	// Protocol defines the protocol used for data retrieval.
	Protocol() multicodec.Code
	// PayloadLength defines how many bytes the binary encoding of the payload
	// of this protocol takes up.
	PayloadLength() int
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Equal(other ProtocolMetadata) bool
}

var (
	_ encoding.BinaryMarshaler   = (*Metadata)(nil)
	_ encoding.BinaryUnmarshaler = (*Metadata)(nil)
)

// Equal determines if two Metadata values are equal.
func (m Metadata) Equal(other Metadata) bool {
	if len(m.Protocols) != len(other.Protocols) {
		return false
	}
	sort.Slice(m.Protocols, func(i, j int) bool {
		return m.Protocols[i].Protocol() < m.Protocols[j].Protocol()
	})
	sort.Slice(other.Protocols, func(i, j int) bool {
		return other.Protocols[i].Protocol() < other.Protocols[j].Protocol()
	})
	for i := range m.Protocols {
		if !m.Protocols[i].Equal(other.Protocols[i]) {
			return false
		}
	}
	return true
}

// Protocols returns the parsed protocols
func (m *Metadata) Codes() []multicodec.Code {
	protocols := make([]multicodec.Code, len(m.Protocols))
	for i, p := range m.Protocols {
		protocols[i] = p.Protocol()
	}
	return protocols
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (m Metadata) MarshalBinary() ([]byte, error) {
	if len(m.Protocols) == 0 {
		return nil, &ErrInvalidMetadata{Message: "encountered nil metadata on encode"}
	}
	sort.Slice(m.Protocols, func(i, j int) bool {
		return m.Protocols[i].Protocol() < m.Protocols[j].Protocol()
	})

	buf := bytes.Buffer{}
	for i := range m.Protocols {
		p := varint.ToUvarint(uint64(m.Protocols[i].Protocol()))
		if _, err := buf.Write(p); err != nil {
			return nil, err
		}

		si, err := m.Protocols[i].MarshalBinary()
		if err != nil {
			return nil, err
		}
		if _, err = buf.Write(si); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func MetadataFromBytes(data []byte) (*Metadata, error) {
	m := Metadata{
		Protocols: make([]ProtocolMetadata, 0),
	}
	if err := m.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &m, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (m *Metadata) UnmarshalBinary(data []byte) error {
	l := 0
	for l < len(data) {
		protocol, protoLen, err := varint.FromUvarint(data[l:])
		if err != nil {
			return err
		}
		if protocol == 0 {
			return &ErrInvalidMetadata{Message: "encountered protocol ID 0 on decode"}
		}
		l += protoLen
		factory, ok := defaultRegistry[multicodec.Code(protocol)]
		if !ok {
			// okay if there are protocols we don't know about
			return nil
		}
		proto := factory()
		if err := proto.UnmarshalBinary(data[l:]); err != nil {
			return err
		}
		m.Protocols = append(m.Protocols, proto)
		l += proto.PayloadLength()
	}

	return nil
}
