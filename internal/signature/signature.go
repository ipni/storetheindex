package signature

import (
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
)

const NonceLen = 16

// VerifySignature verifies the signature over the given data using the public
// key from the given peerID
func Verify(peerID peer.ID, data, signature []byte) error {
	if len(data) == 0 {
		return errors.New("no signed data")
	}

	if len(signature) == 0 {
		return errors.New("empty signature")
	}

	pubKey, err := peerID.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("could not extract public key from peer id: %s", err)
	}

	ok, err := pubKey.Verify(data, signature)
	if err != nil {
		return fmt.Errorf("could not verify signature: %s", err)
	}

	if !ok {
		return errors.New("invalid signature")
	}

	return nil
}

// Nonce generates NonceLen random bytes
func Nonce() ([]byte, error) {
	b := make([]byte, NonceLen)
	_, err := rand.Read(b)
	if err != nil {
		return nil, fmt.Errorf("could not create nonce: %s", err)
	}
	return b, nil
}
