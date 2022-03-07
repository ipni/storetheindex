package v0_test

import (
	"testing"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/util"
)

func TestEncodeDecode(t *testing.T) {

	origMetadata := v0.Metadata{
		Protocols: []v0.ProtocolMetadata{
			&util.ExampleMetadata{Data: []byte("test-data")},
		},
	}
	metadataBytes, err := origMetadata.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if len(metadataBytes) == 0 {
		t.Fatal("did not encode metadata")
	}

	metadata, err := v0.MetadataFromBytes(metadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(metadata.Codes()) != 1 {
		t.Fatal("expected 1 protocol")
	}
	if metadata.Codes()[0] != origMetadata.Codes()[0] {
		t.Fatal("got wrong protocol ID")
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
}
