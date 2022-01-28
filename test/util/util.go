package util

import (
	"math/rand"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
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
