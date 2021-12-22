package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
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
				log.Infow("Received advertisement", "cid", c)

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

				// Register or update provider info with addresses from
				// advertisement.
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
				err = reg.RegisterOrUpdate(provID, addrs, c)
				if err != nil {
					return err
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
				// Persist the advertisement.  This is read later when
				// processing each chunk of entries, to get info common to all
				// entries in a chunk.
				return ds.Put(dsKey(c.String()), origBuf)
			}
			log.Debug("Persisting IPLD node")
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

//func (ing *Ingester) makeStorageHook() graphsync.OnIncomingBlockHook {
//	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
//		ing.storageHook(blockData.Link().(cidlink.Link).Cid)
//	}
//}

// storageHook determines the logic to run when a new block is received through
// graphsync.
//
// When we receive a block, if it is not an advertisement it means that we
// finished storing the list of entries of the advertisement, so we are ready
// to process them and ingest into the indexer core.
func (ing *Ingester) storageHook(p peer.ID, c cid.Cid) {
	log := log.With("peerID", p, "cid", c)
	log.Debug("Incoming block hook triggered")

	// Get data corresponding to the block.
	val, err := ing.ds.Get(dsKey(c.String()))
	if err != nil {
		log.Errorw("Error while fetching the node from datastore", "err", err)
		return
	}

	// Decode block to IPLD node
	node, err := decodeIPLDNode(bytes.NewBuffer(val))
	if err != nil {
		log.Errorw("Error decoding ipldNode", "err", err)
		return
	}

	// If this is an advertisement, sync entries within it.
	if isAdvertisement(node) {
		log.Debug("Incoming block is an advertisement")
		ad, err := decodeAd(node)
		if err != nil {
			log.Errorw("Error decoding advertosement", "err", err)
			return
		}
		log.Infow("Syncing content blocks for advertisement", "ad_cid", c)

		// TODO: consider exposing config value for maximum sync timeout then set in context.
		err = ing.syncAdEntries(context.TODO(), p, ad)
		if err != nil {
			log.Errorw("Error syncing advertosement entries", "err", err)
		}
		return
	}

	// The incoming block is not an advertisement.  This means it is a
	// block of content CIDs to index.  Get the advertisement CID
	// corresponding to the link CID, from the reverse map.  Then load the
	// advertisement to get the metadata for indexing all the content in
	// the incoming block.
	log.Debug("Incoming block is not an advertisement; processing entries")
	adCid, err := getCidToAdMapping(ing.ds, c)
	if err != nil {
		log.Errorw("Error getting advertisementID for entries map", "err", err)
		return
	}
	log = log.With("adCid", adCid)

	log.Infow("Indexing content in block", "ad_cid", adCid)
	err = ing.indexContentBlock(adCid, p, node)
	if err != nil {
		log.Errorw("Error processing entries for advertisement", "err", err)
		return
	}

	ing.signalMetricsUpdate()

	// Remove the datastore entry that maps a chunk to an advertisement
	// now that the chunk is processed.
	log.Debug("Removing mapping to advertisement for processed entries")
	err = deleteCidToAdMapping(ing.ds, c)
	if err != nil {
		log.Errorw("Error deleting cid-advertisement mapping for entries", "err", err)
	}

	// Remove the content block from the data store now that processing it
	// has finished.  This prevents storing redundant information in
	// several datastores.
	log.Debug("Removing processed entries from datastore")
	err = ing.ds.Delete(dsKey(c.String()))
	if err != nil {
		log.Errorw("Error deleting index from datastore", "err", err)
	}
}

// syncAdEntries fetches all the entries for a single advertisement
func (ing *Ingester) syncAdEntries(ctx context.Context, from peer.ID, ad schema.Advertisement) error {
	log := log.With("peerID", from)

	elink, err := ad.FieldEntries().AsLink()
	if err != nil {
		log.Errorw("Error decoding advertisement entries link", "err", err)
		return err
	}
	entriesCid := elink.(cidlink.Link).Cid

	log = log.With("entriesCid", entriesCid)
	exists, err := ing.ds.Has(dsKey(entriesCid.String()))
	if err != nil && err != datastore.ErrNotFound {
		log.Errorf("Error chekcing if entries exist ", "err", err)
		return err
	}
	if exists {
		log.Debugw("Entries already exist; skipping sync")
		return nil
	}

	log.Info("Starting sync for entries")
	go func() {
		startTime := time.Now()
		// Fully traverse the entries, because:
		//  * if the head is not persisted locally there is a chance we do not have it.
		//  * chain of entries as specified by EntryChunk schema only contain entries.
		_, err = ing.sub.Sync(ctx, from, entriesCid, selectorparse.CommonSelector_ExploreAllRecursively, nil)
		if err != nil {
			log.Errorw("Failed to sync", "err", err)
			return
		}
		elapsed := time.Since(startTime)
		// Record how long sync tool.
		stats.Record(context.Background(), metrics.SyncLatency.M(float64(elapsed.Nanoseconds())/1e6))
		log.Infow("Finished syncing entries", "elapsed", elapsed)
	}()
	return nil
}

// indexContentBlock indexes the content CIDs in a block of data.  First the
// advertisement is loaded to get the context ID and metadata, and then that
// and the CIDs in the content block are indexed by the indexer-core.
func (ing *Ingester) indexContentBlock(adCid cid.Cid, p peer.ID, nentries ipld.Node) error {
	// Getting the advertisement for the entries so we know
	// what metadata and related information we need to use for ingestion.
	adb, err := ing.ds.Get(dsKey(adCid.String()))
	if err != nil {
		log.Errorw("Error while fetching advertisement for entry", "err", err)
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
	metadataBytes, err := ad.FieldMetadata().AsBytes()
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

	// Check for valid metadata
	err = new(v0.Metadata).UnmarshalBinary(metadataBytes)
	if err != nil {
		log.Errorf("Error decoding metadata: %s", err)
		return err
	}

	value := indexer.Value{
		ProviderID:    p,
		ContextID:     contextID,
		MetadataBytes: metadataBytes,
	}

	mhChan := make(chan multihash.Multihash, ing.batchSize)
	// TODO: Once we change the syncing process, there may never be a need
	// to remove individual entries, and only a need remove all entries for
	// the context ID in the advertisement.  For now, handle both cases.
	errChan := ing.batchIndexerEntries(mhChan, value, isRm)

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
			close(mhChan)
			return err
		}

		select {
		case mhChan <- h:
		case err = <-errChan:
			return err
		}

		count++
	}
	close(mhChan)
	err = <-errChan
	if err != nil {
		return err
	}

	// Handle remove in the case where there are no individual entries.
	if isRm && count == 0 {
		err = ing.indexer.RemoveProviderContext(p, contextID)
		if err != nil {
			return err
		}
	}

	// If there is a next link, update the mapping so we know the AdID
	// it is related to.
	if !(nchunk.Next.IsAbsent() || nchunk.Next.IsNull()) {
		lnk, err := nchunk.Next.AsNode().AsLink()
		if err != nil {
			return err
		}
		err = putCidToAdMapping(ing.ds, lnk, adCid)
		if err != nil {
			return err
		}
	}

	return nil
}

// batchIndexerEntries starts a goroutine that processes batches of multihashes
// from an input channels.  The goroutine collects these into a slice, storing
// up to batchSize elements.  When the slice is at capacity, a Put or Remove
// request is made to the indexer core depending on the whether isRm is true
// or false.  This function returns an error channel that returns an error if
// one occurs during processing.  This also indicates the goroutine has exited
// (and will no longer read its input channel).
//
// The goroutine exits when the input channel is closed.  It closes the error
// channel to indicate completion.
func (ing *Ingester) batchIndexerEntries(mhChan <-chan multihash.Multihash, value indexer.Value, isRm bool) <-chan error {
	var indexFunc func(indexer.Value, ...multihash.Multihash) error
	var opName string
	if isRm {
		indexFunc = ing.indexer.Remove
		opName = "remove"
	} else {
		indexFunc = ing.indexer.Put
		opName = "put"
	}

	errChan := make(chan error, 1)

	go func(batchSize int) {
		defer close(errChan)
		batch := make([]multihash.Multihash, 0, batchSize)
		var count int
		for m := range mhChan {
			batch = append(batch, m)
			if len(batch) == batchSize {
				// Process full batch of multihashes
				if err := indexFunc(value, batch...); err != nil {
					errChan <- err
					log.Errorf("Cannot %s entries in indexer: %s", opName, err)
					return
				}
				batch = batch[:0]
				count += batchSize
				log.Debugf("%s %d entries in value store", opName, batchSize)
			}
		}

		if len(batch) != 0 {
			// Process any remaining puts
			if err := indexFunc(value, batch...); err != nil {
				errChan <- err
				log.Errorf("Cannot %s entries in indexer: %s", opName, err)
				return
			}
			count += len(batch)
			log.Debugf("%s %d entries in value store", opName, len(batch))
		}

		log.Debugw("Processed entries", "count", count, "operation", opName)
	}(ing.batchSize)

	return errChan
}

func putCidToAdMapping(ds datastore.Batching, lnk ipld.Link, adCid cid.Cid) error {
	return ds.Put(dsKey(admapPrefix+lnk.(cidlink.Link).Cid.String()), adCid.Bytes())
}

func getCidToAdMapping(ds datastore.Batching, linkCid cid.Cid) (cid.Cid, error) {
	val, err := ds.Get(dsKey(admapPrefix + linkCid.String()))
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot read advertisement CID for entries CID from datastore: %s", err)
	}
	adCid, err := cid.Cast(val)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot cast advertisement CID: %s", err)
	}
	return adCid, nil
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
