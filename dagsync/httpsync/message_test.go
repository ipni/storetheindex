package httpsync

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/require"
)

func TestRoundTripSignedHead(t *testing.T) {
	privKey, pubKey, err := ic.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err, "Err generarting private key")

	testCid, err := cid.Parse("bafybeicyhbhhklw3kdwgrxmf67mhkgjbsjauphsvrzywav63kn7bkpmqfa")
	require.NoError(t, err, "Err parsing cid")

	signed, err := newEncodedSignedHead(testCid, privKey)
	require.NoError(t, err, "Err creating signed envelope")

	cidRT, err := openSignedHead(pubKey, bytes.NewReader(signed))
	require.NoError(t, err, "Err opening msg envelope")

	require.Equal(t, testCid, cidRT, "CidStr mismatch. Failed round trip")
}

func TestRoundTripSignedHeadWithIncludedPubKey(t *testing.T) {
	privKey, pubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err, "Err generarting private key")

	testCid, err := cid.Parse("bafybeicyhbhhklw3kdwgrxmf67mhkgjbsjauphsvrzywav63kn7bkpmqfa")
	require.NoError(t, err, "Err parsing cid")

	signed, err := newEncodedSignedHead(testCid, privKey)
	require.NoError(t, err, "Err creating signed envelope")

	includedPubKey, head, err := openSignedHeadWithIncludedPubKey(bytes.NewReader(signed))
	require.NoError(t, err, "Err opening msg envelope")

	require.Equal(t, testCid, head, "CidStr mismatch. Failed round trip")

	require.Equal(t, pubKey, includedPubKey, "pubkey mismatch. Failed round trip")

	// Try with a pubkey that doesn't match
	_, otherPubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err, "Err generarting other key")

	_, err = openSignedHead(otherPubKey, bytes.NewReader(signed))
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid signature",
		"Expected an error when opening envelope with another pubkey. And the error should be 'invalid signature'")
}
