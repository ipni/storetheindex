package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
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

var (
	errBadAdvert              = errors.New("bad advertisement")
	errInvalidAdvertSignature = errors.New("invalid advertisement signature")
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
		val, err := ds.Get(lctx.Ctx, dsKey(c.String()))
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
				return errors.New("bad ipld data")
			}
			// If it is an advertisement.
			if isAdvertisement(n) {
				log.Infow("Received advertisement", "cid", c)

				// Verify that the signature is correct and the advertisement
				// is valid.
				ad, provID, err := verifyAdvertisement(n)
				if err != nil {
					return err
				}

				addrs, err := schema.IpldToGoStrings(ad.FieldAddresses())
				if err != nil {
					log.Errorw("Could not get addresses from advertisement", err)
					return errBadAdvert
				}

				// Register provider or update existing registration.  The
				// provider must be allowed by policy to be registered.
				err = reg.RegisterOrUpdate(lctx.Ctx, provID, addrs, c)
				if err != nil {
					return err
				}

				// Store entries link into the reverse map so there is a way of
				// identifying what advertisementID announced these entries
				// when we come across the link.
				log.Debug("Setting reverse map for entries after receiving advertisement")
				elnk, err := ad.FieldEntries().AsLink()
				if err != nil {
					log.Errorw("Error getting link for entries from advertisement", "err", err)
					return errBadAdvert
				}
				err = putCidToAdMapping(ds, elnk, c)
				if err != nil {
					log.Errorw("Error storing reverse map for entries in datastore", "err", err)
					return errors.New("cannot process advertisement")
				}

				log.Debug("Persisting new advertisement")
				// Persist the advertisement.  This is read later when
				// processing each chunk of entries, to get info common to all
				// entries in a chunk.
				return ds.Put(lctx.Ctx, dsKey(c.String()), origBuf)
			}
			log.Debug("Persisting IPLD node")
			// Any other type of node (like entries) are stored right away.
			return ds.Put(lctx.Ctx, dsKey(c.String()), origBuf)
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

func verifyAdvertisement(n ipld.Node) (schema.Advertisement, peer.ID, error) {
	ad, err := decodeAd(n)
	if err != nil {
		log.Errorw("Cannot decode advertisement", "err", err)
		return nil, peer.ID(""), errBadAdvert
	}
	// Verify advertisement signature
	signerID, err := schema.VerifyAdvertisement(ad)
	if err != nil {
		// stop exchange, verification of signature failed.
		log.Errorw("Advertisement signature verification failed", "err", err)
		return nil, peer.ID(""), errInvalidAdvertSignature
	}

	// Get provider ID from advertisement.
	provider, err := ad.FieldProvider().AsString()
	if err != nil {
		log.Errorw("Cannot read provider from advertisement", "err", err)
		return nil, peer.ID(""), errBadAdvert
	}
	provID, err := peer.Decode(provider)
	if err != nil {
		log.Errorw("Cannot decode provider ID", "err", err)
		return nil, peer.ID(""), errBadAdvert
	}

	// Verify that the advertised provider has signed, and
	// therefore approved, the advertisement regardless of who
	// published the advertisement.
	if signerID != provID {
		// TODO: Have policy that allows a signer (publisher) to
		// sign advertisements for certain providers.  This will
		// allow that signer to add, update, and delete indexed
		// content on behalf of those providers.
		log.Errorw("Advertisement not signed by provider", "provider", provID, "signer", signerID)
		return nil, peer.ID(""), errInvalidAdvertSignature
	}

	log.Infow("Advertisement signature is valid", "provider", provID)

	return ad, provID, nil
}

// storageHook determines the logic to run when a new block is received through
// graphsync.
//
// When an advertisement block is received, it means that all the
// advertisements, from the most recent to the last seen, have been traversed
// and stored.  Now this hook is being called for each advertisement.  Start a
// background sync for the chain of entries in the advertisement.
//
// When a non-advertisement block is received, it means that all the entries
// for an advertisement have been traversed and stored.  Now this hook is being
// called for each entry in an advertisement's chain of entries.  Process the
// entry and save all the multihashes in it as indexes in the indexer-core.
func (ing *Ingester) storageHook(pubID peer.ID, c cid.Cid) {
	log := log.With("publisher", pubID, "adCid", c)
	log.Debug("Incoming block hook triggered")

	// Get data corresponding to the block.
	val, err := ing.ds.Get(context.Background(), dsKey(c.String()))
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
			log.Errorw("Error decoding advertisement", "err", err)
			return
		}

		// It is possible that entries for a previous advertisement are still being
		// synced, and the current advertisement deletes those entries or updates
		// the metadata.  The delete or update does not need to wait on a sync, and
		// so may be done before the previous advertisement is finished syncing.
		// The continued sync of the previous advertisement could undo the delete
		// or update.
		//
		// Therefore it is necessary to ensure the ad syncs execute in the order
		// that the advertisements occur on their chain.  This is done using a
		// lockChain to lock the current advertisement and wait for the previous
		// advertisement to finish being processed and unlock.
		var prevCid cid.Cid
		if ad.FieldPreviousID().Exists() {
			lnk, err := ad.FieldPreviousID().Must().AsLink()
			if err != nil {
				log.Errorw("Cannot read previous link from advertisement", "err", err)
			} else {
				prevCid = lnk.(cidlink.Link).Cid
			}
		}
		// Signal this ad is busy and wait for any sync on the previous ad to
		// finish.  Signal this ad is done at function return.
		prevWait, unlock, err := ing.adLocks.lockWait(prevCid, c)
		if err != nil {
			log.Error("Advertisement already being synced")
			return
		}

		go ing.syncAdEntries(pubID, ad, c, prevWait, unlock)
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

	log.Infow("Indexing content in block")
	err = ing.indexContentBlock(adCid, pubID, node)
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
	err = ing.ds.Delete(context.Background(), dsKey(c.String()))
	if err != nil {
		log.Errorw("Error deleting index from datastore", "err", err)
	}
}

// syncAdEntries fetches all the entries for a single advertisement
func (ing *Ingester) syncAdEntries(from peer.ID, ad schema.Advertisement, adCid cid.Cid, prevWait <-chan struct{}, unlock context.CancelFunc) {
	<-prevWait
	defer unlock()

	log := log.With("publisher", from, "adCid", adCid)

	log.Infow("Syncing content for advertisement")

	isRm, err := ad.FieldIsRm().AsBool()
	if err != nil {
		log.Errorw("Cannot read IsRm field", "err", err)
		return
	}

	contextID, err := ad.FieldContextID().AsBytes()
	if err != nil {
		log.Error("Cannot read context ID from advertisement", "err", err)
		return
	}

	// If advertisement is for removal and has a contextID, then remove
	// everything from the indexer-core with the contextID and provider from
	// this advertisement.
	if isRm && len(contextID) != 0 {
		provider, err := ad.FieldProvider().AsString()
		if err != nil {
			log.Errorw("cannot read provider from advertisement", "err", err)
			return
		}
		providerID, err := peer.Decode(provider)
		if err != nil {
			log.Errorw("Cannot decode provider peer id", "err", err)
			return
		}
		log := log.With("contextID", base64.StdEncoding.EncodeToString(contextID), "provider", providerID)
		err = ing.indexer.RemoveProviderContext(providerID, contextID)
		if err != nil {
			log.Error("Failed to removed content by context ID")
			return
		}
		log.Infow("Removed content by context ID")
		return
	}

	if isRm {
		log.Warnw("Syncing content blocks to remove for removal advertisement with no context ID")
	} else {
		log.Infow("Syncing content blocks for advertisement")
	}

	elink, err := ad.FieldEntries().AsLink()
	if err != nil {
		log.Errorw("Error decoding advertisement entries link", "err", err)
		return
	}
	entriesCid := elink.(cidlink.Link).Cid

	if entriesCid == cid.Undef {
		log.Error("Advertisement entries link is undefined and ad is not removal")
		return
	}

	log = log.With("entriesCid", entriesCid)
	exists, err := ing.ds.Has(context.Background(), dsKey(entriesCid.String()))
	if err != nil && err != datastore.ErrNotFound {
		log.Errorw("Failed checking if entries exist", "err", err)
	}
	if exists {
		log.Debugw("Entries already exist; skipping sync")
		return
	}

	log.Info("Starting sync for entries")
	ctx := context.Background()
	if ing.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer cancel()
	}
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
	// Record how long sync took.
	stats.Record(context.Background(), metrics.SyncLatency.M(float64(elapsed.Nanoseconds())/1e6))
	log.Infow("Finished syncing entries", "elapsed", elapsed)
}

// indexContentBlock indexes the content CIDs in a block of data.  First the
// advertisement is loaded to get the context ID and metadata, and then that
// and the CIDs in the content block are indexed by the indexer-core.
//
// The pubID is the peer ID of the message publisher.  This is not necessarily
// the same as the provider ID in the advertisement.  The publisher is the
// source of the indexed content, the provider is where content can be
// retrieved from.  It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) indexContentBlock(adCid cid.Cid, pubID peer.ID, nentries ipld.Node) error {
	// Getting the advertisement for the entries so we know
	// what metadata and related information we need to use for ingestion.
	adb, err := ing.ds.Get(context.Background(), dsKey(adCid.String()))
	if err != nil {
		return fmt.Errorf("cannot read advertisement for entry from datastore: %s", err)
	}
	// Decode the advertisement.
	adn, err := decodeIPLDNode(bytes.NewBuffer(adb))
	if err != nil {
		return fmt.Errorf("cannot decode ipld node: %s", err)
	}
	ad, err := decodeAd(adn)
	if err != nil {
		return fmt.Errorf("cannot decode advertisement: %s", err)
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

	// The peerID passed into the storage hook is the source of the
	// advertisement (the publisher), and not necessarily the same as the
	// provider in the advertisement.  Read the provider from the advertisement
	// to create the indexed value.
	provider, err := ad.FieldProvider().AsString()
	if err != nil {
		return fmt.Errorf("cannot read provider from advertisement: %s", err)
	}
	providerID, err := peer.Decode(provider)
	if err != nil {
		return fmt.Errorf("cannot decode provider peer id: %s", err)
	}

	// Decode the list of cids into a List_String
	nb := schema.Type.EntryChunk.NewBuilder()
	err = nb.AssignNode(nentries)
	if err != nil {
		return fmt.Errorf("cannot decode entries: %s", err)
	}

	// Check for valid metadata
	err = new(v0.Metadata).UnmarshalBinary(metadataBytes)
	if err != nil {
		return fmt.Errorf("cannot decoding metadata: %s", err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadataBytes,
	}

	mhChan := make(chan multihash.Multihash, ing.batchSize)
	// The isRm parameter is passed in for an advertisement that contains
	// entries, to allow for removal of individual entries.
	errChan := ing.batchIndexerEntries(mhChan, value, isRm)

	var count int
	nchunk := nb.Build().(schema.EntryChunk)
	entries := nchunk.FieldEntries()
	// Iterate over all entries and ingest (or remove) them.
	cit := entries.ListIterator()
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		if err != nil {
			close(mhChan)
			return fmt.Errorf("cannot decode an entry from the ingestion list: %s", err)
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
	return ds.Put(context.Background(), dsKey(admapPrefix+lnk.(cidlink.Link).Cid.String()), adCid.Bytes())
}

func getCidToAdMapping(ds datastore.Batching, linkCid cid.Cid) (cid.Cid, error) {
	val, err := ds.Get(context.Background(), dsKey(admapPrefix+linkCid.String()))
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
	return ds.Delete(context.Background(), dsKey(admapPrefix+entries.String()))
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(r io.Reader) (ipld.Node, error) {
	// NOTE: Considering using the schema prototypes.  This was failing, using
	// a map gives flexibility.  Maybe is worth revisiting this again in the
	// future.
	nb := basicnode.Prototype.Any.NewBuilder()
	err := dagjson.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// Checks if an IPLD node is an advertisement, by looking to see if it has a
// "Signature" field.  We may need additional checks if we extend the schema
// with new types that are traversable.
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}
