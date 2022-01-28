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
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"

	// Import so these codecs get registered.
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

type adCacheItem struct {
	value indexer.Value
	isRm  bool
}

var waitPreviousAdTime = 5 * time.Second

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
func mkLinkSystem(ds datastore.Batching, reg *registry.Registry, adWaiter *cidWaiter) ipld.LinkSystem {
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
			codec := lnk.(cidlink.Link).Prefix().Codec
			origBuf := buf.Bytes()

			log := log.With("cid", c)

			// Decode the node to check its type.
			n, err := decodeIPLDNode(codec, buf)
			if err != nil {
				log.Errorw("Error decoding IPLD node in linksystem", "err", err)
				return errors.New("bad ipld data")
			}
			// If it is an advertisement.
			if isAdvertisement(n) {
				log.Infow("Received advertisement")

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

				// Persist the advertisement.  This is read later when
				// processing each chunk of entries, to get info common to all
				// entries in a chunk.
				return ds.Put(lctx.Ctx, dsKey(c.String()), origBuf)
			}
			log.Debug("Received IPLD node")
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
	provID, err := providerFromAd(ad)
	if err != nil {
		log.Errorw("Cannot get provider from advertisement", "err", err)
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

	return ad, provID, nil
}

// providerFromAd reads the provider ID from an advertisement
func providerFromAd(ad schema.Advertisement) (peer.ID, error) {
	provider, err := ad.FieldProvider().AsString()
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot read provider from advertisement: %s", err)
	}

	providerID, err := peer.Decode(provider)
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot decode provider peer id: %s", err)
	}

	return providerID, nil
}

// storageHook determines the logic to run when a new block is received through
// graphsync.
//
// For a chain of advertisements, the storage hook sees the advertisements from
// newest to oldest, and starts an entries sync goroutine for each
// advertisement.  These goroutines each wait for the sync goroutine associated
// with the previous advertisement to complete before beginning their sync for
// content blocks.
//
// When a non-advertisement block is received, it means that the entries sync
// is running and is collecting content chunks for an advertisement.  Now this
// hook is being called for each entry in an advertisement's chain of entries.
// Process the entry and save all the multihashes in it as indexes in the
// indexer-core.
func (ing *Ingester) storageHook(pubID peer.ID, c cid.Cid) {
	log := log.With("publisher", pubID, "cid", c)

	// Get data corresponding to the block.
	val, err := ing.ds.Get(context.Background(), dsKey(c.String()))
	if err != nil {
		log.Errorw("Error while fetching the node from datastore", "err", err)
		return
	}

	// Decode block to IPLD node
	node, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val))
	if err != nil {
		log.Errorw("Error decoding ipldNode", "err", err)
		return
	}

	// If this is an advertisement, sync entries within it.
	if isAdvertisement(node) {
		ad, err := decodeAd(node)
		if err != nil {
			log.Errorw("Error decoding advertisement", "err", err)
			return
		}

		// Store entries link into the reverse map so there is a way of
		// identifying what advertisementID announced these entries
		// when we come across the link.
		log.Debug("Saving map of entries to advertisement and advertisement data")
		elnk, err := ad.FieldEntries().AsLink()
		if err != nil {
			log.Errorw("Error getting link for entries from advertisement", "err", err)
		}

		err = pushCidToAdMapping(context.Background(), ing.ds, elnk.(cidlink.Link).Cid, c)
		if err != nil {
			log.Errorw("Error storing reverse map for entries in datastore", "err", err)
		}

		// Make the CID of this advertisement waitable.  When content
		// chunks for an advertisement arrive (in storage hook),
		// processing will wait for the previous advertisement to
		// finish processing.  The CID is added to the waiter here, so
		// that it is available for waiting on before the arrival of
		// the previous advertisement's content blocks.
		err = ing.adWaiter.add(c)
		if err != nil {
			log.Errorw("Cannot create cid wait for advertisement", "err", err)
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
				return
			}
			prevCid = lnk.(cidlink.Link).Cid
		}

		log.Infow("Incoming block is an advertisement", "prevAd", prevCid)

		go ing.syncAdEntries(pubID, ad, c, prevCid)
		return
	}

	// Get the advertisement CID corresponding to the link CID, from the
	// reverse map.  Then load the advertisement to get the metadata for
	// indexing all the content in the incoming block.
	adCid, err := popCidToAdMapping(context.Background(), ing.ds, c)
	if err != nil {
		log.Errorw("Error getting advertisement CID for entry CID", "err", err)
		return
	}
	log = log.With("adCid", adCid)

	// The incoming block is not an advertisement.  This means it is a
	// block of content to index.
	log.Info("Incoming block is a content chunk - indexing content")

	err = ing.indexContentBlock(adCid, pubID, node)
	if err != nil {
		log.Errorw("Error processing entries for advertisement", "err", err)
	} else {
		log.Info("Done indexing content in entry block")
		ing.signalMetricsUpdate()
	}

	// Remove the content block from the data store now that processing it
	// has finished.  This prevents storing redundant information in
	// several datastores.
	err = ing.ds.Delete(context.Background(), dsKey(c.String()))
	if err != nil {
		log.Errorw("Error deleting index from datastore", "err", err)
	}
}

// syncAdEntries fetches all the entries for a single advertisement
func (ing *Ingester) syncAdEntries(from peer.ID, ad schema.Advertisement, adCid, prevCid cid.Cid) {
	if !ing.adWaiter.wait(prevCid) {
		// If there was a previous ad, but it was not waitiable, then check if
		// is it already in the datastore.  It ad is not stored, then wait for
		// it to become waitable.  If ad is already in the datastoreto, then
		// was previously processed, so do not wait.
		exists, err := ing.ds.Has(context.Background(), dsKey(prevCid.String()))
		if err != nil {
			log.Errorw("Failed checking if ad exist", "err", err)
		}
		if !exists {
			time.Sleep(waitPreviousAdTime)
		}
		ing.adWaiter.wait(prevCid)
	}

	defer ing.adWaiter.done(adCid)
	log := log.With("publisher", from, "adCid", adCid)

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

	elink, err := ad.FieldEntries().AsLink()
	if err != nil {
		log.Errorw("Error decoding advertisement entries link", "err", err)
		return
	}

	// If advertisement has no entries, then this is for removal by contextID
	// or for updating metadata only.
	if elink == schema.NoEntries {
		providerID, err := providerFromAd(ad)
		if err != nil {
			log.Errorw("Invalid advertisement", "err", err)
			return
		}

		log := log.With("contextID", base64.StdEncoding.EncodeToString(contextID), "provider", providerID)

		if isRm {
			log.Infow("Advertisement is for removal by context id")

			err = ing.indexer.RemoveProviderContext(providerID, contextID)
			if err != nil {
				log.Error("Failed to removed content by context ID")
			}
			return
		}

		// If this is a metadata update only, then ad will not have entries.
		metadataBytes, err := ad.FieldMetadata().AsBytes()
		if err != nil {
			log.Errorw("Error reading advertisement metadata", "err", err)
			return
		}

		value := indexer.Value{
			ContextID:     contextID,
			MetadataBytes: metadataBytes,
			ProviderID:    providerID,
		}

		log.Error("Advertisement is metadata update only")
		ing.indexer.Put(value)
		return
	}

	entriesCid := elink.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		log.Error("Advertisement entries link is undefined")
		return
	}
	log = log.With("entriesCid", entriesCid)

	exists, err := ing.ds.Has(context.Background(), dsKey(entriesCid.String()))
	if err != nil && err != datastore.ErrNotFound {
		log.Errorw("Failed checking if entries exist", "err", err)
	}
	if exists {
		log.Info("Entries already exist; skipping sync")
		return
	}

	if isRm {
		log.Warnw("Syncing content entries for removal advertisement with no context ID")
	} else {
		log.Infow("Syncing content entries for advertisement")
	}

	// Cleanup ad cache in case of failure during processing entries.
	defer func() {
		ing.adCacheMutex.Lock()
		delete(ing.adCache, adCid)
		ing.adCacheMutex.Unlock()
	}()

	ctx := context.Background()
	if ing.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer cancel()
	}
	startTime := time.Now()
	// Traverse entries based on the entries selector that limits recursion depth.
	_, err = ing.sub.Sync(ctx, from, entriesCid, ing.entriesSel, nil)
	if err != nil {
		log.Errorw("Failed to sync", "err", err)
		return
	}
	elapsed := time.Since(startTime)
	// Record how long sync took.
	stats.Record(context.Background(), metrics.SyncLatency.M(float64(elapsed.Nanoseconds())/1e6))
	log.Infow("Finished syncing entries", "elapsed", elapsed)

	// We've processed all the entries, we can mark this ad as processed
	err = ing.markAdProcessed(from, adCid)
	if err != nil {
		log.Errorf("Failed to mark ad as processed: %v", err)
	} else {
		log.Debugw("Persisted latest sync", "peer", from, "cid", adCid)
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(tag.Insert(metrics.Method, "libp2p2")),
			stats.WithMeasurements(metrics.IngestChange.M(1)))

		ing.signalMetricsUpdate()
	}
}

// indexContentBlock indexes the content multihashes in a block of data.  First
// the advertisement is loaded to get the context ID and metadata.  Then the
// metadata and multihashes in the content block are indexed by the
// indexer-core.
//
// The pubID is the peer ID of the message publisher.  This is not necessarily
// the same as the provider ID in the advertisement.  The publisher is the
// source of the indexed content, the provider is where content can be
// retrieved from.  It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) indexContentBlock(adCid cid.Cid, pubID peer.ID, nentries ipld.Node) error {
	// Decode the list of cids into a List_String
	nb := schema.Type.EntryChunk.NewBuilder()
	err := nb.AssignNode(nentries)
	if err != nil {
		return fmt.Errorf("cannot decode entries: %s", err)
	}

	nchunk := nb.Build().(schema.EntryChunk)

	// If this entry chunk hash a next link, add a mapping from the next
	// entries CID to the ad CID so that the ad can be loaded when that chunk
	// is received.
	//
	// If we error in reading the next link, then that means this entry chunk is
	// malformed, so it is okay to skip it.
	hasNextLink, err := ing.setNextCidToAd(nchunk, adCid)
	if err != nil {
		return err
	}

	// Load the advertisement data for this chunk.  If there are more chunks to
	// follow, then cache the ad data.
	value, isRm, err := ing.loadAdData(adCid, hasNextLink)
	if err != nil {
		return err
	}

	mhChan := make(chan multihash.Multihash, ing.batchSize)
	// The isRm parameter is passed in for an advertisement that contains
	// entries, to allow for removal of individual entries.
	errChan := ing.batchIndexerEntries(mhChan, value, isRm)

	// Iterate over all entries and ingest (or remove) them.
	entries := nchunk.FieldEntries()
	cit := entries.ListIterator()
	var count int
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
		if isRm {
			return fmt.Errorf("cannot remove multihashes from indexer: %s", err)
		}
		return fmt.Errorf("cannot put multihashes into indexer: %s", err)
	}

	err = <-errChan
	if err != nil {
		return nil
	}

	return nil

}

func (ing *Ingester) setNextCidToAd(nchunk schema.EntryChunk, adCid cid.Cid) (bool, error) {
	if nchunk.Next.IsAbsent() || nchunk.Next.IsNull() {
		// Chunk has no next link
		return false, nil
	}

	lnk, err := nchunk.Next.AsNode().AsLink()
	if err != nil {
		return false, fmt.Errorf("error decoding next chunk link: %w", err)
	}
	err = pushCidToAdMapping(context.Background(), ing.ds, lnk.(cidlink.Link).Cid, adCid)
	if err != nil {
		return false, fmt.Errorf("error storing reverse map for next chunk: %w", err)
	}

	return true, nil
}

func (ing *Ingester) loadAdData(adCid cid.Cid, keepCache bool) (indexer.Value, bool, error) {
	ing.adCacheMutex.Lock()
	adData, ok := ing.adCache[adCid]
	if !keepCache && ok {
		if len(ing.adCache) == 1 {
			ing.adCache = nil
		} else {
			delete(ing.adCache, adCid)
		}
	}
	ing.adCacheMutex.Unlock()

	if ok {
		return adData.value, adData.isRm, nil
	}

	// Getting the advertisement for the entries so we know
	// what metadata and related information we need to use for ingestion.
	adb, err := ing.ds.Get(context.Background(), dsKey(adCid.String()))
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot read advertisement for entry from datastore: %s", err)
	}
	// Decode the advertisement.
	adn, err := decodeIPLDNode(adCid.Prefix().Codec, bytes.NewBuffer(adb))
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decode ipld node: %s", err)
	}
	ad, err := decodeAd(adn)
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decode advertisement: %s", err)
	}
	// Fetch data of interest.
	contextID, err := ad.FieldContextID().AsBytes()
	if err != nil {
		return indexer.Value{}, false, err
	}
	metadataBytes, err := ad.FieldMetadata().AsBytes()
	if err != nil {
		return indexer.Value{}, false, err
	}
	isRm, err := ad.FieldIsRm().AsBool()
	if err != nil {
		return indexer.Value{}, false, err
	}

	// The peerID passed into the storage hook is the source of the
	// advertisement (the publisher), and not necessarily the same as the
	// provider in the advertisement.  Read the provider from the advertisement
	// to create the indexed value.
	providerID, err := providerFromAd(ad)
	if err != nil {
		return indexer.Value{}, false, err
	}

	// Check for valid metadata
	err = new(v0.Metadata).UnmarshalBinary(metadataBytes)
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decoding metadata: %s", err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadataBytes,
	}

	if keepCache {
		ing.adCacheMutex.Lock()
		if ing.adCache == nil {
			ing.adCache = make(map[cid.Cid]adCacheItem)
		}
		ing.adCache[adCid] = adCacheItem{
			value: value,
			isRm:  isRm,
		}
		ing.adCacheMutex.Unlock()
	}

	return value, isRm, nil
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
					return
				}
				batch = batch[:0]
				count += batchSize
			}
		}

		if len(batch) != 0 {
			// Process any remaining puts
			if err := indexFunc(value, batch...); err != nil {
				errChan <- err
				return
			}
			count += len(batch)
		}

		log.Infow("Processed multihashes in entry chunk", "count", count, "operation", opName)
	}(ing.batchSize)

	return errChan
}

func pushCidToAdMapping(ctx context.Context, ds datastore.Batching, entCid, adCid cid.Cid) error {
	log.Debugw("Push reverse mapping", "entryCid", entCid, "adCid", adCid)
	dk := dsKey(admapPrefix + entCid.String())

	data, err := ds.Get(ctx, dk)
	if err != nil || len(data) == 0 {
		// No previous data, so just write this CID.
		err = ds.Put(ctx, dk, adCid.Bytes())
	} else {
		// There is already a CID, so prepend this CID.
		var buf bytes.Buffer
		buf.Grow(adCid.ByteLen() + len(data))
		buf.Write(adCid.Bytes())
		buf.Write(data)
		err = ds.Put(ctx, dk, buf.Bytes())
	}
	if err != nil {
		return err
	}

	return ds.Sync(ctx, dsKey(admapPrefix))
}

func popCidToAdMapping(ctx context.Context, ds datastore.Batching, linkCid cid.Cid) (cid.Cid, error) {
	dk := dsKey(admapPrefix + linkCid.String())
	data, err := ds.Get(ctx, dk)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot load advertisement cid for entries cid from datastore: %s", err)
	}

	n, adCid, err := cid.CidFromBytes(data)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot decode advertisement cid: %s", err)
	}
	data = data[n:]
	// If no remaining data, delete mapping.  Otherwise write remaining data
	// back to datastore.
	if len(data) == 0 {
		err = ds.Delete(ctx, dk)
		if err != nil {
			log.Errorw("Failed to delete entries cid to advertisement cit mapping", "err", err, "linkCid", linkCid)
		}
	} else {
		err = ds.Put(ctx, dk, data)
		if err != nil {
			log.Errorw("Failed to write remaining entries to advertisement mapping", "err", err, "linkCid", linkCid)
		}
	}

	log.Debugw("Pop reverse mapping", "entryCid", linkCid, "adCid", adCid)
	return adCid, nil
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(codec uint64, r io.Reader) (ipld.Node, error) {
	// NOTE: Considering using the schema prototypes.  This was failing, using
	// a map gives flexibility.  Maybe is worth revisiting this again in the
	// future.
	nb := basicnode.Prototype.Any.NewBuilder()
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		return nil, err
	}
	err = decoder(nb, r)
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
