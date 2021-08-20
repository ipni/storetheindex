package ingestion

import (
	"bytes"
	"io"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/utils"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ipld "github.com/ipld/go-ipld-prime"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/test"

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

func genIndexAndAdv(t *testing.T, lsys ipld.LinkSystem,
	priv crypto.PrivKey, cids []cid.Cid,
	rmcids []cid.Cid) (Index, Link_Index, Advertisement, Link_Advertisement) {

	p, _ := utils.TestPeerID()
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	index, indexLnk, err := NewSingleEntryIndex(lsys, cids, nil, val.Metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	adv, advLnk, err := NewAdvertisement(lsys, priv, nil, indexLnk, p.String(), true)
	if err != nil {
		t.Fatal(err)
	}
	return index, indexLnk, adv, advLnk
}

func TestChainAdvertisements(t *testing.T) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	dstore := datastore.NewMapDatastore()
	lsys := mkLinkSystem(dstore)
	p, _ := utils.TestPeerID()
	cids, _ := utils.RandomCids(10)
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	// Genesis index
	index, indexLnk, err := NewSingleEntryIndex(lsys, cids, nil, val.Metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	if index.FieldPrevious().v.x != nil {
		t.Error("previous should be nil, it's the genesis", index.Previous.v)
	}
	// Genesis advertisement
	adv, _, err := NewAdvertisement(lsys, priv, nil, indexLnk, p.String(), true)
	if err != nil {
		t.Fatal(err)
	}
	if adv.FieldPreviousID().x != nil {
		t.Error("previous should be nil, it's the genesis", index.Previous.v)
	}
	// Seecond node
	index2, indexLnk2, err := NewSingleEntryIndex(lsys, cids, nil, val.Metadata, indexLnk)
	if err != nil {
		t.Fatal(err)
	}
	if index2.FieldPrevious().v.x != indexLnk.x {
		t.Error("index2 should be pointing to genesis", index2.FieldPrevious().v.x, indexLnk.x)
	}
	// Genesis advertisement
	adv2, _, err := NewAdvertisement(lsys, priv, adv.FieldID().x, indexLnk2, p.String(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(adv2.FieldPreviousID().x, adv.FieldID().x) {
		t.Error("adv2 should be pointing to genesis", adv2.FieldPreviousID().x, adv.FieldID().x)
	}
}

func TestAdvSignature(t *testing.T) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	dstore := datastore.NewMapDatastore()
	lsys := mkLinkSystem(dstore)
	cids, _ := utils.RandomCids(10)
	_, _, adv, _ := genIndexAndAdv(t, lsys, priv, cids, cids)

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
