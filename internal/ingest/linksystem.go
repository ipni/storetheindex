package ingest

import (
	"bytes"
	"io"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

func dsKey(k string) datastore.Key {
	return datastore.NewKey(k)
}

// mkLinkSystem makes the indexer linkSystem which checks advertisement
// signatures at storage. If the signature is not valid the traversal/exchange
// is terminated.
func mkLinkSystem(ds datastore.Batching, reg *registry.Registry) ipld.LinkSystem {
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
				log.Errorf("Error decoding IPLD node in linksystem: %s", err)
				return err
			}
			// If it is an advertisement.
			if isAdvertisement(n) {
				// Verify if the signature is correct.
				// And the advertisement valid.
				ad, err := verifyAdvertisement(n)
				if err != nil {
					log.Errorf("Error verifying if node is of type advertisement: %s", err)
					return err
				}

				addrs, err := schema.IpldToGoStrings(ad.FieldAddresses())
				if err != nil {
					log.Error("Could not get addresses from advertisement")
					return syserr.New(err, http.StatusBadRequest)
				}

				// If addresses are included with the advertisement
				if len(addrs) != 0 {
					provider, err := ad.FieldProvider().AsString()
					if err != nil {
						log.Errorf("Could not get provider from advertisement: %s", err)
						return err
					}

					provID, err := peer.Decode(provider)
					if err != nil {
						log.Errorf("Could not decode advertisement provider ID: %s", err)
						return syserr.New(err, http.StatusBadRequest)
					}

					err = reg.RegisterOrUpdate(provID, addrs)
					if err != nil {
						return err
					}
				}

				// Store entries link into the reverse map
				// so we have a way of identifying what advertisementID
				// announced these entries when we come across the link
				log.Debug("Setting reverse map for entries after receiving advertisement")
				elnk, err := ad.FieldEntries().AsLink()
				if err != nil {

					log.Errorf("Error getting link for entries from advertisement: %s", err)
					return err
				}
				err = putCidToAdMapping(ds, elnk, c)
				if err != nil {
					log.Errorf("Error storing reverse map for entries in datastore: %s", err)
					return err
				}

				log.Debug("Persisting new advertisement")
				// Persist the advertisement
				return ds.Put(dsKey(c.String()), origBuf)
			}
			log.Debug("Persisting an IPLD node not of type advertisement")
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
		log.Errorf("Error decoding advertisement: %s", err)
		return nil, err
	}
	// Verify advertisement signature
	if err := schema.VerifyAdvertisement(ad); err != nil {
		// stop exchange, verification of signature failed.
		log.Errorf("Signature verification failed for advertisement: %s", err)
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
		log.Debug("hook - Triggering hooko after a block has been stored")
		// Get cid of the node received.
		c := blockData.Link().(cidlink.Link).Cid

		// Get entries node from datastore.
		val, err := i.ds.Get(dsKey(c.String()))
		if err != nil {
			log.Errorf("Error while fetching the node from datastore: %s", err)
			return
		}

		// Decode entries into an IPLD node
		nentries, err := decodeIPLDNode(bytes.NewBuffer(val))
		if err != nil {
			log.Errorf("Error decoding ipldNode: %s", err)
			return
		}

		// If it is not an advertisement, is the list of Cids.
		// Let's ingest it!
		if !isAdvertisement(nentries) {
			// Get the advertisement ID corresponding to the link.
			// From the reverse map.
			log.Debug("hook - Not an advertisement, let's start ingesting entries")
			val, err := i.ds.Get(dsKey(admapPrefix + c.String()))
			if err != nil {
				log.Errorf("Error while fetching the advertisementID for entries map: %s", err)
			}
			adCid, err := cid.Cast(val)
			if err != nil {
				log.Errorf("Error casting Cid for advertisement: %s", err)
			}

			log.Debug("hook - Processing entries from an advertisement")
			// Process entries and ingest them.
			err = i.processEntries(adCid, p, nentries)
			if err != nil {
				log.Errorf("Error processing entries for advertisement: %s", err)

			}

			// We can remove the datastore entry between chunk and CID once
			// we've process it.
			err = deleteCidToAdMapping(i.ds, c)
			if err != nil {
				log.Errorf("Error deleting cid-advertisement mapping for entries: %s", err)
			}

			log.Debug("hook - Removing entries from datastore to prevent entries from being stored redundantly")
			// When we are finished processing the index we can remove
			// it from the datastore (we don't want redundant information
			// in several datastores).
			err = i.ds.Delete(dsKey(c.String()))
			if err != nil {
				log.Errorf("Error deleting index from datastore: %s", err)
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
		log.Errorf("Error while fetching advertisement for entry: %s", err)
		return err
	}
	// Decode the advertisement.
	adn, err := decodeIPLDNode(bytes.NewBuffer(adb))
	if err != nil {
		log.Errorf("Error decoding ipldNode: %s", err)
		return err
	}
	ad, err := decodeAd(adn)
	if err != nil {
		log.Errorf("Error decoding advertisement: %s", err)
		return err
	}
	// Fetch data of interest.
	contextID, err := ad.FieldContextID().AsBytes()
	if err != nil {
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
	// check here if needed.
	// provider, err := ad.FieldProvider().AsString()

	// Decode the list of cids into a List_String
	nb := schema.Type.EntryChunk.NewBuilder()
	err = nb.AssignNode(nentries)
	if err != nil {
		log.Errorf("Error decoding entries: %s", err)
		return err
	}

	value := indexer.MakeValue(p, contextID, 0, metadata)

	var putChan, removeChan chan multihash.Multihash
	var errChan <-chan error
	if i.batchSize > 1 {
		putChan = make(chan multihash.Multihash, i.batchSize)
		removeChan = make(chan multihash.Multihash, i.batchSize)
		errChan = batchIndexerEntries(i.batchSize, putChan, removeChan, value, i.indexer)
	}

	var count int
	nchunk := nb.Build().(schema.EntryChunk)
	entries := nchunk.FieldEntries()
	// Iterate over all entries and ingest them
	cit := entries.ListIterator()
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		if err != nil {
			log.Errorf("Error decoding an entry from the ingestion list: %s", err)
			if i.batchSize > 1 {
				close(putChan)
				close(removeChan)
			}
			return err
		}

		if isRm {
			// TODO: Remove will change once we change the syncing process
			// because we may not receive the list of CIDs to remove and we'll
			// have to use a routine that looks for the CIDs for a specific
			// key.
			if i.batchSize > 1 {
				select {
				case removeChan <- h:
				case err = <-errChan:
				}
			} else {
				err = i.indexer.Remove(value, h)
			}
		} else {
			if i.batchSize > 1 {
				select {
				case putChan <- h:
				case err = <-errChan:
				}
			} else {
				err = i.indexer.Put(value, h)
			}
		}
		if err != nil {
			return err
		}

		count++
	}
	if i.batchSize > 1 {
		close(putChan)
		close(removeChan)
		err = <-errChan
		if err != nil {
			return err
		}
	}
	log.Debugw("Processed entries", "count", count)

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

// batchIndexerEntries starts a goroutine that reads multihashes from two input
// channels, putChan and removeChan.  The goroutine collects these into
// separate slices, storing up to batchSize elements.  When a slice is at
// capacity, a Put or Remove request is made to the indexer core.  This
// function returns an error channel that returns an error if one occurs during
// Put or Remove, which also indicates the goroutine has exited (and will no
// longer read its input channels).
//
// The goroutine exits when both the input channels are closed.  It closes the
// error channel to indicate completion.
func batchIndexerEntries(batchSize int, putChan, removeChan <-chan multihash.Multihash, value indexer.Value, idxr indexer.Interface) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		puts := make([]multihash.Multihash, 0, batchSize)
		removes := make([]multihash.Multihash, 0, batchSize)
		for {
			select {
			case m, open := <-putChan:
				if !open {
					if len(puts) != 0 {
						// Process any remaining puts
						if err := idxr.Put(value, puts...); err != nil {
							errChan <- err
							log.Errorf("Error putting entries in indexer: %s", err)
							return
						}
					}
					if removeChan == nil {
						// All input channels closed
						return
					}
					putChan = nil
					continue
				}
				puts = append(puts, m)
				if len(puts) == batchSize {
					// Process full batch of puts
					if err := idxr.Put(value, puts...); err != nil {
						errChan <- err
						log.Errorf("Error putting entries in indexer: %s", err)
						return
					}
					puts = puts[:0]
				}
			case m, open := <-removeChan:
				if !open {
					if len(removes) != 0 {
						// Process any remaining removes
						if err := idxr.Remove(value, removes...); err != nil {
							log.Errorf("Error removing entries from indexer: %s", err)
							errChan <- err
							return
						}
					}
					if putChan == nil {
						// All input channels closed
						return
					}
					removeChan = nil
					continue
				}
				removes = append(removes, m)
				if len(removes) == batchSize {
					// Process full batch of removes
					if err := idxr.Remove(value, removes...); err != nil {
						errChan <- err
						log.Errorf("Error removing entries from indexer: %s", err)
						return
					}
					removes = removes[:0]
				}
			}
		}
	}()

	return errChan
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
