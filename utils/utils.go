package utils

import (
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var prefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1, // default length
}

func RandomCids(n int) ([]cid.Cid, error) {
	var prng = rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		res[i] = c
	}
	return res, nil
}
