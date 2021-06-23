package utils

import (
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var pref = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1, // default length
}

func RandomCids(n int) ([]cid.Cid, error) {
	rand.Seed(time.Now().UnixNano())
	res := make([]cid.Cid, 0)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		rand.Read(b)
		c, err := pref.Sum(b)
		if err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil

}
