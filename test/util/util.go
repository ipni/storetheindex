package util

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/filecoin-project/go-indexer-core"
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

	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		mhashes[i] = c.Hash()
	}
	return mhashes, nil
}

func EqualMultihashes(m1, m2 []multihash.Multihash) bool {
	if len(m1) != len(m2) {
		return false
	}
	for i := range m1 {
		if !bytes.Equal([]byte(m1[i]), []byte(m2[i])) {
			return false
		}
	}
	return true
}

func EqualValues(vals1, vals2 []indexer.Value) bool {
	if len(vals1) != len(vals2) {
		return false
	}
	for i := range vals1 {
		if !HasValue(vals2, vals1[i]) {
			return false
		}
	}
	return true
}

func HasValue(values []indexer.Value, v indexer.Value) bool {
	for i := range values {
		if values[i].Equal(v) {
			return true
		}
	}
	return false
}
