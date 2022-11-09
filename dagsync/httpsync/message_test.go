package httpsync

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	ic "github.com/libp2p/go-libp2p/core/crypto"
)

func TestRoundTripSignedHead(t *testing.T) {
	privKey, pubKey, err := ic.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal("Err generarting private key", err)
	}

	testCid, err := cid.Parse("bafybeicyhbhhklw3kdwgrxmf67mhkgjbsjauphsvrzywav63kn7bkpmqfa")
	if err != nil {
		t.Fatal("Err parsing cid", err)
	}

	signed, err := newEncodedSignedHead(testCid, privKey)
	if err != nil {
		t.Fatal("Err creating signed envelope", err)
	}

	cidRT, err := openSignedHead(pubKey, bytes.NewReader(signed))
	if err != nil {
		t.Fatal("Err Opening msg envelope", err)
	}

	if cidRT != testCid {
		t.Fatal("CidStr mismatch. Failed round trip")
	}
}

func TestRoundTripSignedHeadWithIncludedPubKey(t *testing.T) {
	privKey, pubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal("Err generarting private key", err)
	}

	testCid, err := cid.Parse("bafybeicyhbhhklw3kdwgrxmf67mhkgjbsjauphsvrzywav63kn7bkpmqfa")
	if err != nil {
		t.Fatal("Err parsing cid", err)
	}

	signed, err := newEncodedSignedHead(testCid, privKey)
	if err != nil {
		t.Fatal("Err creating signed envelope", err)
	}

	includedPubKey, head, err := openSignedHeadWithIncludedPubKey(bytes.NewReader(signed))
	if err != nil {
		t.Fatal("Err Opening msg envelope", err)
	}

	if head != testCid {
		t.Fatal("CidStr mismatch. Failed round trip")
	}

	if !includedPubKey.Equals(pubKey) {
		t.Fatal("pubkey mismatch. Failed round trip")
	}

	// Try with a pubkey that doesn't match
	_, otherPubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal("Err generarting other key", err)
	}

	_, err = openSignedHead(otherPubKey, bytes.NewReader(signed))
	if err == nil || err.Error() != "invalid signature" {
		t.Fatal("Expected an error when opening envelope with another pubkey. And the error should be 'invalid signature'")
	}
}
