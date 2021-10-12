package util

import (
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func RandomMultihashes(n int) ([]multihash.Multihash, error) {
	prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, 64)
	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		mhashes[i] = c.Hash()
	}
	return mhashes, nil
}
