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
	metadataBytes, err := origMetadata.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if len(metadataBytes) == 0 {
		t.Fatal("did not encode metadata")
	}

	var metadata Metadata
	if err := metadata.UnmarshalBinary(metadataBytes); err != nil {
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

	// Zero the bytes and ensure the decoded struct still works.
	// This will fail if UnmarshalBinary did not copy the inner data bytes.
	copy(metadataBytes, make([]byte, 1024))
	if !metadata.Equal(origMetadata) {
		t.Fatal("metadata no equal after buffer zeroing")
	}

	metadata.ProtocolID = origMetadata.ProtocolID + 1
	if metadata.Equal(origMetadata) {
		t.Fatal("metadata should not be equal")
	}
}
