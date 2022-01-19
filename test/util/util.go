package util

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"time"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	testProtocolID = 0x300000
)

func RandomMultihashes(n int) []multihash.Multihash {
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return RandomMultihashesFromRand(n, prng)
}

func RandomMultihashesFromRand(n int, prng *rand.Rand) []multihash.Multihash {
	prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}

	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n+16)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err.Error())
		}
		mhashes[i] = c.Hash()
	}
	return mhashes
}

func RandomAdvChain(priv crypto.PrivKey, lsys ipld.LinkSystem, numberOfMhChunksInEachAdv []uint8) (schema.Link_Advertisement, error) {
	var prevAdvLink schema.Link_Advertisement
	p, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	for i, numberOfMhsChunks := range numberOfMhChunksInEachAdv {
		r := rand.New(rand.NewSource(int64(numberOfMhsChunks)))
		mhsLnk, mhs, err := newRandomLinkedListWithErr(r, lsys, int(numberOfMhsChunks))
		if err != nil {
			return nil, err
		}

		ctxID := []byte("test-context-id" + fmt.Sprint(i))
		metadata := v0.Metadata{
			ProtocolID: testProtocolID,
			Data:       mhs[0],
		}
		addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
		_, prevAdvLink, err = schema.NewAdvertisementWithLink(lsys, priv, prevAdvLink, mhsLnk, ctxID, metadata, false, p.String(), addrs)
		if err != nil {
			return nil, err
		}
	}

	return prevAdvLink, nil
}

func newRandomLinkedListWithErr(rand *rand.Rand, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash, error) {
	out := []multihash.Multihash{}
	mhs := RandomMultihashesFromRand(10, rand)
	out = append(out, mhs...)
	nextLnk, _, err := schema.NewLinkedListOfMhs(lsys, mhs, nil)
	if err != nil {
		return nil, nil, err
	}

	for i := 1; i < size; i++ {
		if i == 2 {
			mhs = RandomMultihashesFromRand(10, rand)
		}
		nextLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, nextLnk)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, mhs...)
	}
	return nextLnk, out, err
}

func NewProvLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}
