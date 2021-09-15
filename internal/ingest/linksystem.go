package ingest

import (
	"bytes"
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

// mkLinkSystem makes the indexer linkSystem which checks advertisement
// signatures at storage. If the signature is not valid the traversal/exchange
// is terminated.
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
			origBuf := buf.Bytes()
			// Decode the node to check its type.
			n, err := decodeIPLDNode(buf)
			if err != nil {
				return err
			}
			// If it is an advertisement.
			if isAdvertisement(n) {
				// Verify if the signature is correct.
				// And the advertisement valid.
				ad, err := verifyAdvertisement(n)
				if err != nil {
					return err
				}

				// Store entries link into the reverse map
				// so we have a way of identifying what advertisementID
				// announced these entries when we come across the link
				elnk, err := ad.FieldEntries().AsLink()
				if err != nil {
					return err
				}
				err = putCidToAdMapping(ds, elnk, c)
				if err != nil {
					return err
				}

				// Persist the advertisement
				return ds.Put(dsKey(c.String()), origBuf)
			}
			// Any other type of node (like entries) are stored right away.
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
		log.Errorf("Signature verification failed for advertisement: %v", err)
		return nil, err
	}
	return ad, nil
}

// storageHook determines the logic to run when a new block is received through
// graphsync.
//
// When we receive a block, if it is not an advertisement it means that we
// finished storing the list of entries of the advertisement, so we are ready
// to process them and ingest into the indexer core.
func (i *legIngester) storageHook() graphsync.OnIncomingBlockHook {
	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		// Get cid of the node received.
		c := blockData.Link().(cidlink.Link).Cid

		// Get entries node from datastore.
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
			// From the reverse map.
			val, err := i.ds.Get(dsKey(admapPrefix + c.String()))
			if err != nil {
				log.Errorf("Error while fetching the advertisementID for entries map: %v", err)
			}
			adCid, err := cid.Cast(val)
			if err != nil {
				log.Errorf("Error casting Cid for advertisement: %v", err)
			}

			// Process entries and ingest them.
			err = i.processEntries(adCid, p, nentries)
			if err != nil {
				log.Errorf("Error processing entries for advertisement: %v", err)

			}

			// We can remove the datastore entry between chunk and CID once
			// we've process it.
			err = deleteCidToAdMapping(i.ds, c)
			if err != nil {
				log.Errorf("Error deleting cid-advertisement mapping for entries: %v", err)
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
	// Getting the advertisement for the entries so we know
	// what metadata and related information we need to use for ingestion.
	adb, err := i.ds.Get(dsKey(adCid.String()))
	if err != nil {
		log.Errorf("Error while fetching advertisement for entry: %v", err)
		return err
	}
	// Decode the advertisement.
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
	// Fetch data of interest.
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
	// check here if needed.
	// provider, err := ad.FieldProvider().AsString()

	// Decode the list of cids into a List_String
	nb := schema.Type.EntryChunk.NewBuilder()
	err = nb.AssignNode(nentries)
	if err != nil {
		log.Errorf("Error decoding entries: %v", err)
		return err
	}
	nchunk := nb.Build().(schema.EntryChunk)
	entries := nchunk.FieldEntries()
	// Iterate over all entries and ingest them
	cit := entries.ListIterator()
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		if err != nil {
			log.Errorf("Error decoding an entry from the ingestion list: %v", err)
			return err
		}
		val := indexer.MakeValue(p, 0, metadata)
		if isRm {
			// TODO: Remove will change once we change the syncing process
			// because we may not receive the list of CIDs to remove and we'll
			// have to use a routine that looks for the CIDs for a specific
			// key.
			if _, err := i.indexer.Remove(h, val); err != nil {
				log.Errorf("Error removing entry from indexer: %v", err)
				return err
			}

		} else {
			if _, err := i.indexer.Put(h, val); err != nil {
				log.Errorf("Error putting entry in indexer: %v", err)
				return err
			}
		}
		log.Debugf("Success processing entry", "multihash", h)
	}

	// If there is a next link, update the mapping so we know the AdID
	// it is related to.
	if !(nchunk.Next.IsAbsent() || nchunk.Next.IsNull()) {
		lnk, err := nchunk.Next.AsNode().AsLink()
		if err != nil {
			return err
		}
		err = putCidToAdMapping(i.ds, lnk, adCid)
		if err != nil {
			return err
		}
	}

	return nil
}

func putCidToAdMapping(ds datastore.Batching, lnk ipld.Link, adCid cid.Cid) error {
	return ds.Put(dsKey(admapPrefix+lnk.(cidlink.Link).Cid.String()), adCid.Bytes())
}

func deleteCidToAdMapping(ds datastore.Batching, entries cid.Cid) error {
	return ds.Delete(dsKey(admapPrefix + entries.String()))
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
