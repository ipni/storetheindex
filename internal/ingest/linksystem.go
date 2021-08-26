package ingest

import (
	"bytes"
	"fmt"
	"io"

	"github.com/filecoin-project/go-indexer-core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/json"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func mkVanillaLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
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

// Creates the main engine linksystem.
// TODO: This is the linksystem that will eventually fetch the nodes
// and in the fly index the data being received.
func (i *legIngester) mkLinkSystem(p peer.ID) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := i.ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			fmt.Println("peer", p)
			fmt.Println("lctx", lctx)
			if lctx.Ctx != nil {
				fmt.Println("Link Context:", lctx.Ctx.Value(schema.IsIndexKey))
				fmt.Println("peer", p)
				fmt.Println("path", lctx.LinkPath.String(), "node", lctx.LinkNode)
				if bool(lctx.Ctx.Value(schema.IsIndexKey).(schema.LinkContextValue)) {
					index, err := decodeIndexNode(buf)
					if err != nil {
						return err
					}
					err = i.processCidsIndex(p, index)
					if err != nil {
						return err
					}
				}
			}
			c := lnk.(cidlink.Link).Cid
			return i.ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func decodeIndexNode(r io.Reader) (ipld.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	// TODO: Why it fails with schema prototype
	// nb := schema.Type.Index.NewBuilder()
	err := json.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func (i *legIngester) processCidsIndex(p peer.ID, n ipld.Node) error {
	// Get entries
	entries, err := n.LookupByString("CidEntries")
	if err != nil {
		return err
	}
	it := entries.ListIterator()
	for {
		_, e, err := it.Next()
		if err != nil {
			return err
		}
		err = i.processEntry(p, e)
		if err != nil {
			return err
		}
		if it.Done() {
			break
		}
	}
	return nil
}

func (i *legIngester) processEntry(p peer.ID, n ipld.Node) error {
	meta, err := n.LookupByString("Metadata")
	if err != nil {
		return err
	}
	metadata, _ := meta.AsBytes()
	putCids, _ := n.LookupByString("Put")
	cit := putCids.ListIterator()
	for !cit.Done() {
		_, cnode, err := cit.Next()
		if err != nil {
			return err
		}
		cs, _ := cnode.AsString()
		c, err := cid.Decode(cs)
		if err != nil {
			return err
		}
		val := indexer.MakeValue(p, 0, metadata)
		i.indexer.Put(c, val)
		fmt.Println("cidPut", cs)
	}

	rmCids, _ := n.LookupByString("Remove")
	cit = rmCids.ListIterator()
	for !cit.Done() {
		_, cnode, err := cit.Next()
		if err != nil {
			return err
		}
		cs, _ := cnode.AsString()
		c, err := cid.Decode(cs)
		if err != nil {
			return err
		}
		val := indexer.MakeValue(p, 0, metadata)
		_, err = i.indexer.Remove(c, val)
		if err != nil {
			return err
		}
		fmt.Println("cidRemove", cs)
	}
	return nil
}
