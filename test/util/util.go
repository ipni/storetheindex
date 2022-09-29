package util

import (
	"math/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func RandomMultihashes(n int, rng *rand.Rand) []multihash.Multihash {
	prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}

	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n+16)
		rng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err.Error())
		}
		mhashes[i] = c.Hash()
	}
	return mhashes
}

func RandomIdentity(t *testing.T) (peer.ID, crypto.PrivKey, crypto.PubKey) {
	privKey, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	providerID, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)
	return providerID, privKey, pubKey
}

func StringToMultiaddrs(t *testing.T, addrs []string) []multiaddr.Multiaddr {
	mAddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		require.NoError(t, err)
		mAddrs[i] = ma
	}
	return mAddrs
}
