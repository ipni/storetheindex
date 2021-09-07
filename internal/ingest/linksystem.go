package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-indexer-core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func dsKey(k string) datastore.Key {
	return datastore.NewKey(k)
}

// mkVanillaLinkSystem makes a standard vanilla linkSystem that stores and loads from a datastore.
func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(dsKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			// Store the advertisement
			origBuf := buf.Bytes()
			n, err := decodeIPLDNode(buf)
			if err != nil {
				return err
			}
			if isAdvertisement(n) {
				// Verify if the signature is correct.
				// And the advertisement valid.
				ad, err := verifyAdvertisement(n)
				if err != nil {
					return err
				}
				/*
					// Store advertisement and mapping between
					// advertisement and link of entries.
					r := bytes.NewBuffer(nil)
					err = dagjson.Encode(ad, r)
					if err != nil {
						return err
					}
				*/
				// Store entries link to the map
				elnk, err := ad.FieldEntries().AsLink()
				if err != nil {
					return err
				}
				err = ds.Put(dsKey("adMap/"+elnk.(cidlink.Link).Cid.String()), c.Bytes())
				if err != nil {
					return err
				}
				// Store the advertisement
				return ds.Put(dsKey(c.String()), origBuf)
			}
			// Store the list of entries.
			return ds.Put(dsKey(c.String()), origBuf)
		}, nil
	}
	return lsys
}

func decodeAd(n ipld.Node) (schema.Advertisement, error) {
	nb := schema.Type.Advertisement.NewBuilder()
	err := nb.AssignNode(n)
	if err != nil {
		return nil, err
	}
	return nb.Build().(schema.Advertisement), nil
}
func verifyAdvertisement(n ipld.Node) (schema.Advertisement, error) {
	ad, err := decodeAd(n)
	if err != nil {
		log.Errorf("Error decoding advertisement: %v", err)
		return nil, err
	}
	// Verify advertisement signature
	if err := schema.VerifyAdvertisement(ad); err != nil {
		// stop exchange, verification of signature failed.
		// hookActions.TerminateWithError(errors.New("advertisement verification failed"))
		log.Errorf("Signature verification failed for add: %v", err)
		return nil, err
	}
	return ad, nil
}

// storageHook determines the logic to run when a new block is received through graphsync.
// NOTE: This hook is run after an exchanged IPLD node has been stored in the datastore.
// This means that the node is persisted and then processed. This is not the most appropriate
// solution as it requires storing and then deleting data instead of processing the stream
// receive in the linkSystem, but until we figure out how to pass to the linkSystem
// the peer invoved in the request, we'll need to go with this approach.
func (i *legIngester) storageHook() graphsync.OnIncomingBlockHook {
	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		// Get cid of the node received.
		c := blockData.Link().(cidlink.Link).Cid
		// Get the entries node from the datastore.
		val, err := i.ds.Get(dsKey(c.String()))
		if err != nil {
			log.Errorf("Error while fetching the node from datastore: %v", err)
			return
		}
		// Decode entries into an IPLD node
		nentries, err := decodeIPLDNode(bytes.NewBuffer(val))
		if err != nil {
			log.Errorf("Error decoding ipldNode: %v", err)
			return
		}

		// If it is not an advertisement, is the list of Cids.
		// Let's ingest it!
		if !isAdvertisement(nentries) {
			// Get the advertisement ID corresponding to the link.
			val, err := i.ds.Get(dsKey("adMap/" + c.String()))
			if err != nil {
				log.Errorf("Error while fetching the advertisementID for entries map: %v", err)
			}
			adCid, err := cid.Cast(val)
			if err != nil {
				log.Errorf("Error casting Cid for advertisement: %v", err)
			}

			err = i.processEntries(adCid, p, nentries)
			if err != nil {
				log.Errorf("Error processing entries for advertisement: %v", err)

			}

			// When we are finished processing the index we can remove
			// it from the datastore (we don't want redundant information
			// in several datastores).
			err = i.ds.Delete(dsKey(c.String()))
			if err != nil {
				log.Errorf("Error deleting index from datastore: %v", err)
				return
			}
		}
	}
}

func (i *legIngester) processEntries(adCid cid.Cid, p peer.ID, nentries ipld.Node) error {
	adb, err := i.ds.Get(dsKey("adMap/" + adCid.String()))
	if err != nil {
		log.Errorf("Error while fetching advertisement for entry: %v", err)
		return err
	}
	adn, err := decodeIPLDNode(bytes.NewBuffer(adb))
	if err != nil {
		log.Errorf("Error decoding ipldNode: %v", err)
		return err
	}
	ad, err := decodeAd(adn)
	if err != nil {
		log.Errorf("Error decoding advertisement: %v", err)
		return err
	}
	metadata, err := ad.FieldMetadata().AsBytes()
	if err != nil {
		return err
	}
	isRm, err := ad.FieldIsRm().AsBool()
	if err != nil {
		return err
	}
	// NOTE: No need to get provider from the advertisement
	// we have in the message source. We could add an additional
	// check here though.
	// provider, err := ad.FieldProvider().AsString()

	nb := schema.Type.List_String.NewBuilder()
	err = nb.AssignNode(nentries)
	if err != nil {
		log.Errorf("Error decoding entries: %v", err)
		return err
	}
	entries := nb.Build()
	// Iterate over all entries
	cit := entries.ListIterator()
	for !cit.Done() {
		_, cnode, err := cit.Next()
		if err != nil {
			// return err
		}
		cs, _ := cnode.AsString()
		c, err := cid.Decode(cs)
		if err != nil {
			return err
		}
		val := indexer.MakeValue(p, 0, metadata)
		if isRm {
			if _, err := i.indexer.Remove(c, val); err != nil {
				log.Errorf("Error removing CID %s in indexer: %v", c, err)
				return err
			}

		} else {
			if _, err := i.indexer.Put(c, val); err != nil {
				log.Errorf("Error putting CID %s in indexer: %v", c, err)
				return err
			}
		}
		log.Debugf("Success putting CID %s in indexer", c)
	}
	return nil
}

// storageHook determines the logic to run when a new block is received through graphsync.
// NOTE: This hook is run after an exchanged IPLD node has been stored in the datastore.
// This means that the node is persisted and then processed. This is not the most appropriate
// solution as it requires storing and then deleting data instead of processing the stream
// receive in the linkSystem, but until we figure out how to pass to the linkSystem
// the peer invoved in the request, we'll need to go with this approach.
func (i *legIngester) storageHook2() graphsync.OnIncomingBlockHook {
	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		// Get cid of the node received.
		c := blockData.Link().(cidlink.Link).Cid
		// Get the node from the datastore.
		val, err := i.ds.Get(dsKey(c.String()))
		if err != nil {
			log.Errorf("Error while fetching the node from datastore: %v", err)
		}

		// Decode into an IPLD node
		n, err := decodeIPLDNode(bytes.NewBuffer(val))
		if err != nil {
			log.Errorf("Error decoding ipldNode: %v", err)
			return
		}

		// Check if it is of type Index (i.e. not an advertisements).
		// Nothing needs to done for advertisements here, just traverse
		// and persist them.
		if isAdvertisement(n) {
			nb := schema.Type.Advertisement.NewBuilder()
			err := nb.AssignNode(n)
			if err != nil {
				log.Errorf("Error decoding advertisement: %v", err)
				return
			}
			ad := nb.Build().(schema.Advertisement)
			// Verify advertisement signature
			if err := schema.VerifyAdvertisement(ad); err != nil {
				// stop exchange, verification of signature failed.
				hookActions.TerminateWithError(errors.New("advertisement verification failed"))
				log.Errorf("Signature verification failed for add: %v", err)
			}

			// When we are finished processing the index we can remove
			// it from the datastore (we don't want redundant information
			// in several datastores).
			err = i.ds.Delete(dsKey(c.String()))
			if err != nil {
				log.Errorf("Error deleting index from datastore: %v", err)
				return
			}
		}
	}
}

// decodeIPLDNode from a reaed
// This is used to get the ipld.Node from a set of raw bytes.
func decodeIPLDNode(r io.Reader) (ipld.Node, error) {
	// NOTE: Considering using the schema prototypes.
	// This was failing, using a map gives flexibility.
	// Maybe is worth revisiting this again in the future.
	nb := basicnode.Prototype.Any.NewBuilder()
	err := dagjson.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// Checks if an IPLD node is an advertisement or
// an index.
// (We may need additional checks if we extend
// the schema with new types that are traversable)
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}

// Process the CIDs included in an IPLD.Node of type index and
// NOTE: We could add a callback to give flexibility to processCidsIndex
// and be able to run a different callbacks according to the needs.
// I don't think it makes sense in this stage.
func (i *legIngester) processCidsIndex(p peer.ID, n ipld.Node, metadata []byte, isRm bool) error {
	fmt.Println(n)
	return nil
}

// Process the CIDs included in an IPLD.Node of type index and
// NOTE: We could add a callback to give flexibility to processCidsIndex
// and be able to run a different callbacks according to the needs.
// I don't think it makes sense in this stage.
func (i *legIngester) processCidsIndex2(p peer.ID, n ipld.Node) error {
	// Get all CidEntries entries
	entries, err := n.LookupByString("CidEntries")
	if err != nil {
		return err
	}

	// Iterate over all entries
	it := entries.ListIterator()
	for {
		_, e, err := it.Next()
		if err != nil {
			return err
		}
		// Process the CIDs of each of the entries.
		err = i.processCidsEntry(p, e)
		if err != nil {
			return err
		}
		if it.Done() {
			break
		}
	}
	return nil
}

// ProcessCidsEntry gets the list of CIDs, and indexes the corresponding
// data in the indexer.
func (i *legIngester) processCidsEntry(p peer.ID, n ipld.Node) error {
	// Get metadata if any.
	meta, err := n.LookupByString("Metadata")
	if err != nil {
		return err
	}
	metadata, err := meta.AsBytes()
	if err != nil {
		return err
	}

	// Get the list of CIDS to put and iterate over them
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
		if _, err := i.indexer.Put(c, val); err != nil {
			log.Errorf("Error putting CID %s in indexer: %v", c, err)
		}
		log.Debugf("Success putting CID %s in indexer", c)
	}

	// Get the list of cids to remove and iterate over them.
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
		if _, err := i.indexer.Remove(c, val); err != nil {
			log.Errorf("Error removing CID %s in indexer: %v", c, err)
		}
		log.Debugf("Success removing CID %s in indexer", c)
	}
	return nil
}

/*
NOTE: Keeping this code for reference here until we settle on the
approach to use to index data as it comes. We currently use a graphsync hook
but we may switch to using a linkSystem in the future
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
					index, err := decodeIPLDNode(buf)
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
*/
