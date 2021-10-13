package schema

import (
	"bytes"
	"io"
	"testing"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-datastore"
	ipld "github.com/ipld/go-ipld-prime"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multihash"

	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

const protocolID = 0x300000

func genCidsAndAdv(t *testing.T, lsys ipld.LinkSystem, priv crypto.PrivKey, previous Link_Advertisement) ([]multihash.Multihash, ipld.Link, Advertisement, Link_Advertisement) {
	mhs := util.RandomMultihashes(10)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	ctxID := []byte("test-context-id")
	metadata := v0.Metadata{
		ProtocolID: protocolID,
		Data:       mhs[0],
	}
	addr := "/ip4/127.0.0.1/tcp/9999"
	cidsLnk, err := NewListOfMhs(lsys, mhs)
	if err != nil {
		t.Fatal(err)
	}
	adv, advLnk, err := NewAdvertisementWithLink(lsys, priv, previous, cidsLnk, ctxID, metadata, false, p.String(), []string{addr})
	if err != nil {
		t.Fatal(err)
	}
	return mhs, cidsLnk, adv, advLnk
}

func TestChainAdvertisements(t *testing.T) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	dstore := datastore.NewMapDatastore()
	lsys := mkLinkSystem(dstore)
	// Genesis advertisement
	_, _, adv1, advLnk1 := genCidsAndAdv(t, lsys, priv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if adv1.FieldPreviousID().v.x != nil {
		t.Error("previous should be nil, it's the genesis", adv1.FieldPreviousID().v.x)
	}
	// Seecond advertisement
	_, _, adv2, advLnk2 := genCidsAndAdv(t, lsys, priv, advLnk1)
	if err != nil {
		t.Fatal(err)
	}
	if adv2.FieldPreviousID().v.x != advLnk1.x {
		t.Error("index2 should be pointing to genesis", adv2.FieldPreviousID().v.x, advLnk2.x)
	}
}

func TestLinkedList(t *testing.T) {
	dstore := datastore.NewMapDatastore()
	lsys := mkLinkSystem(dstore)
	mhs := util.RandomMultihashes(10)
	lnk1, ch1, err := NewLinkedListOfMhs(lsys, mhs, nil)
	if err != nil {
		t.Fatal(err)
	}
	elnk1 := &_Link_EntryChunk{x: lnk1}
	if ch1.FieldNext().v.x != nil {
		t.Fatal("no link should have been assigned")
	}
	lnk2, ch2, err := NewLinkedListOfMhs(lsys, mhs, lnk1)
	if err != nil {
		t.Fatal(err)
	}
	if !ipld.DeepEqual(elnk1, &ch2.FieldNext().v) {
		t.Fatal("elnk1 should equal ch2 fieldNext")
	}
	elnk2 := &_Link_EntryChunk{x: lnk2}
	_, ch3, err := NewLinkedListOfMhs(lsys, mhs, lnk2)
	if err != nil {
		t.Fatal(err)
	}
	if !ipld.DeepEqual(elnk2, &ch3.FieldNext().v) {
		t.Fatal("elnk3 should equal ch2 fieldNext")
	}
}

func TestAdvSignature(t *testing.T) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	dstore := datastore.NewMapDatastore()
	lsys := mkLinkSystem(dstore)
	_, _, adv, _ := genCidsAndAdv(t, lsys, priv, nil)

	// Successful verification
	err = VerifyAdvertisement(adv)
	if err != nil {
		t.Fatal("verification should have been successful", err)
	}

	// Verification fails if something in the advertisement changes
	adv.Provider = _String{x: ""}
	err = VerifyAdvertisement(adv)
	if err == nil {
		t.Fatal("verification should have failed")
	}
}
