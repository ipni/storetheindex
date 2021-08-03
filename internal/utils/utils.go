package utils

import (
	"math/rand"
	"time"

	"github.com/filecoin-project/go-indexer-core/entry"
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

func EqualCids(e1, e2 []cid.Cid) bool {
	if len(e1) != len(e2) {
		return false
	}
	for i := range e1 {
		if !e1[i].Equals(e2[i]) {
			return false
		}
	}
	return true
}

func EqualEntries(e1, e2 []entry.Value) bool {
	if len(e1) != len(e2) {
		return false
	}
	for i := range e1 {
		if !HasEntry(e2, e1[i]) {
			return false
		}
	}
	return true
}

func HasEntry(entries []entry.Value, e entry.Value) bool {
	for i := range entries {
		if entries[i].Equal(e) {
			return true
		}
	}
	return false
}
