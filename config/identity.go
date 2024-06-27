package config

import (
	"encoding/base64"
	"fmt"
	"os"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	IdentityTag     = "Identity"
	PrivKeyTag      = "PrivKey"
	PrivKeySelector = IdentityTag + "." + PrivKeyTag

	PrivateKeyPathEnvVar = "STORETHEINDEX_PRIV_KEY_PATH"
)

// Identity tracks the configuration of the local node's identity.
type Identity struct {
	// PeerID is the peer ID of the indexer that must match the given PrivKey.
	// If unspecified, it is automatically generated from the PrivKey.
	PeerID string
	// PrivKey represents the peer identity of the indexer.
	//
	// If unset, the key is loaded from the file at the path specified via
	// STORETHEINDEX_PRIV_KEY_PATH environment variable.
	PrivKey string `json:",omitempty"`
}

func (i Identity) Decode() (peer.ID, ic.PrivKey, error) {
	privKey, err := i.DecodePrivateKey("")
	if err != nil {
		return "", nil, fmt.Errorf("could not decode private key: %w", err)
	}

	peerIDFromPrivKey, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return "", nil, fmt.Errorf("could not generate peer ID from private key: %w", err)
	}

	// If peer ID is specified in JSON config, then verify that it is:
	//   1. a valid peer ID, and
	//   2. consistent with the peer ID generated from private key.
	if i.PeerID != "" {
		peerID, err := peer.Decode(i.PeerID)
		if err != nil {
			return "", nil, fmt.Errorf("could not decode peer id: %w", err)
		}

		if peerID != "" && peerIDFromPrivKey != peerID {
			return "", nil, fmt.Errorf("provided peer ID must either match the peer ID generated from private key or be omitted: expected %s but got %s", peerIDFromPrivKey, peerID)
		}
	}

	return peerIDFromPrivKey, privKey, nil
}

// DecodePrivateKey is a helper to decode the user's PrivateKey.
func (i Identity) DecodePrivateKey(passphrase string) (ic.PrivKey, error) {
	// TODO(security): currently storing key unencrypted. in the future we need to encrypt it.

	// If a value is supplied in JSON config then attempt to decode it as the private key.
	if i.PrivKey != "" {
		pkb, err := base64.StdEncoding.DecodeString(i.PrivKey)
		if err != nil {
			return nil, err
		}
		return ic.UnmarshalPrivateKey(pkb)
	}

	// Otherwise, load the private key from path supplied via env var.
	privKeyPath := os.Getenv(PrivateKeyPathEnvVar)
	if privKeyPath == "" {
		return nil, fmt.Errorf("private key not specified; it must be specified either in config or via %s env var", PrivateKeyPathEnvVar)
	}

	pkb, err := os.ReadFile(privKeyPath)
	if err != nil {
		return nil, err
	}
	return ic.UnmarshalPrivateKey(pkb)
}
