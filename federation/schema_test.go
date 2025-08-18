package federation

import (
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestHeadSignVerify(t *testing.T) {
	mh, err := multihash.Sum([]byte("fish"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	link := cidlink.Link{Cid: cid.NewCidV1(cid.Raw, mh)}

	t.Run("Correct Signature Verifies", func(t *testing.T) {
		key, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		mpk, err := crypto.MarshalPublicKey(pubKey)
		require.NoError(t, err)

		head := Head{
			Head:      link,
			PublicKey: mpk,
		}
		require.NoError(t, head.Sign(key))
		require.NoError(t, head.Verify(pubKey))
	})

	t.Run("Key Missmatch Fails", func(t *testing.T) {
		key, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		_, anotherPubKey, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)

		mpk, err := crypto.MarshalPublicKey(anotherPubKey)
		require.NoError(t, err)
		head := Head{
			Head:      link,
			PublicKey: mpk,
		}
		require.NoError(t, head.Sign(key))
		require.ErrorContains(t, head.Verify(pubKey), "head public key and signer public keys do not match")
	})
}
