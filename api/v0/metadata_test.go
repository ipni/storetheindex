package v0

import (
	"bytes"
	"testing"
)

const testProtoID = 0x300000

func TestEncodeDecode(t *testing.T) {
	origMetadata := Metadata{
		ProtocolID: testProtoID,
		Data:       []byte("test-data"),
	}
	metadataBytes := origMetadata.Encode()

	if len(metadataBytes) == 0 {
		t.Fatal("did not encode metadata")
	}

	metadata, err := DecodeMetadata(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.ProtocolID != origMetadata.ProtocolID {
		t.Fatal("got wrong protocol ID")
	}
	if !bytes.Equal(metadata.Data, origMetadata.Data) {
		t.Fatal("did not get expected data")
	}
	if !metadata.Equal(origMetadata) {
		t.Fatal("metadata no equal after decode")
	}

	metadata.ProtocolID = origMetadata.ProtocolID + 1
	if metadata.Equal(origMetadata) {
		t.Fatal("metadata should not be equal")
	}
}
