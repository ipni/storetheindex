package util

import (
	"bytes"
	"fmt"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
)

const testProtoID = 0x300000

func init() {
	v0.RegisterMetadataProtocol(func() v0.ProtocolMetadata {
		return &ExampleMetadata{}
	})
}

type ExampleMetadata struct {
	Data []byte
}

func (em *ExampleMetadata) Protocol() multicodec.Code {
	return testProtoID
}

func (em *ExampleMetadata) PayloadLength() int {
	dl := varint.UvarintSize(uint64(len(em.Data)))
	return dl + len(em.Data)
}

func (em *ExampleMetadata) MarshalBinary() ([]byte, error) {
	dl := varint.UvarintSize(uint64(len(em.Data)))
	buf := make([]byte, dl+len(em.Data))
	varint.PutUvarint(buf, uint64(len(em.Data)))
	copy(buf[dl:], em.Data)
	return buf, nil
}

func (em *ExampleMetadata) UnmarshalBinary(data []byte) error {
	l, sl, err := varint.FromUvarint(data)
	if err != nil {
		return err
	}
	if int(l)+sl < len(data) {
		return fmt.Errorf("payload too short")
	}
	em.Data = make([]byte, l)
	copy(em.Data, data[sl:])
	return nil
}

func (em *ExampleMetadata) Equal(other v0.ProtocolMetadata) bool {
	return other.Protocol() == em.Protocol() &&
		bytes.Equal(other.(*ExampleMetadata).Data, em.Data)
}
