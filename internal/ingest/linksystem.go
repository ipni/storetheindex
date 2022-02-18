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
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"

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
func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
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
				_, _, err := verifyAdvertisement(n)
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
		return peer.ID(""), fmt.Errorf("cannot read provider from advertisement: %w", err)
	}

	providerID, err := peer.Decode(provider)
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot decode provider peer id: %w", err)
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
func (ing *Ingester) storageHook(adCid cid.Cid, pubID peer.ID, c cid.Cid) {
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
	stats.Record(context.Background(), metrics.IngestChange.M(1))
	var skip bool
	ingestStart := time.Now()
	defer func() {
		stats.Record(context.Background(), metrics.AdIngestLatency.M(coremetrics.MsecSince(ingestStart)))
	}()

	log := log.With("publisher", from, "adCid", adCid)

	// Get provider ID from advertisement.
	providerID, err := providerFromAd(ad)
	if err != nil {
		log.Errorw("Invalid advertisement", "err", err)
		skip = true
	}

	contextID, err := ad.FieldContextID().AsBytes()
	if err != nil {
		log.Errorw("Cannot read context ID from advertisement", "err", err)
		skip = true
	}

	isRm, err := ad.FieldIsRm().AsBool()
	if err != nil {
		log.Errorw("Cannot read IsRm field", "err", err)
		skip = true
	}

	elink, err := ad.FieldEntries().AsLink()
	if err != nil {
		log.Errorw("Error decoding advertisement entries link", "err", err)
		skip = true
	}

	if !skip {
		skipKey := string(contextID) + providerID.String()
		skip = ing.hasSkip(skipKey)
		if skip {
			// An ad that is later in the chain has deleted all content for
			// this ad, so skip this ad.
			log.Infow("Skipped advertisement that is removed later")
		} else if isRm {
			// This ad deletes all content for a contextID.  So, skip any
			// previous (earlier in chain) ads that arrive later, that have
			// the same contextID and provider.
			ing.addSkip(skipKey)
			// Remove skip after this and all previous ad in chain are processed.
			defer ing.delSkip(skipKey)
		}
	}

	// Mark the ad as processed after done processing. This is even in most
	// error cases so that the indexer is not stuck trying to reprocessing a
	// malformed ad.
	var reprocessAd bool
	defer func() {
		if reprocessAd {
			ing.ds.Delete(context.Background(), dsKey(adCid.String()))
		} else {
			// Processed all the entries, so mark this ad as processed.
			if err := ing.markAdProcessed(from, adCid); err != nil {
				log.Errorw("Failed to mark ad as processed", "err", err)
			} else {
				stats.Record(context.Background(), metrics.AdSyncedCount.M(1))
			}
		}
		// Distribute the legs.SyncFinished notices to waiting Sync calls.
		ing.inEvents <- legs.SyncFinished{Cid: adCid, PeerID: from}
	}()

	// This ad has bad data or has all of its content deleted by a subsequent
	// ad, so skip any further processing.
	if skip {
		if elink != schema.NoEntries {
			entCid := elink.(cidlink.Link).Cid
			_, err := popCidToAdMapping(context.Background(), ing.ds, entCid)
			if err != nil {
				log.Errorw("cannot delete advertisement cid for entries cid from datastore", "err", err)
			}
		}
		return
	}

	addrs, err := schema.IpldToGoStrings(ad.FieldAddresses())
	if err != nil {
		log.Errorw("Could not get addresses from advertisement", "err", err)
		return
	}

	// Register provider or update existing registration.  The
	// provider must be allowed by policy to be registered.
	err = ing.reg.RegisterOrUpdate(context.Background(), providerID, addrs, adCid)
	if err != nil {
		log.Errorw("Could not register/update provider info", "err", err)
		return
	}

	log = log.With("contextID", base64.StdEncoding.EncodeToString(contextID), "provider", providerID)

	if isRm {
		log.Infow("Advertisement is for removal by context id")

		err = ing.indexer.RemoveProviderContext(providerID, contextID)
		if err != nil {
			log.Error("Failed to removed content by context ID")
		}
		return
	}

	// If advertisement has no entries, then this is for updating metadata only.
	if elink == schema.NoEntries {
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

	log.Infow("Syncing content entries for advertisement")

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
	fmt.Println("Starting sync of entries")
	_, err = ing.sub.SyncWithHook(ctx, from, entriesCid, ing.entriesSel, nil, func(p peer.ID, c cid.Cid) {
		fmt.Println("Processing cid")
		ing.storageHook(adCid, p, c)
		fmt.Println("END processing cid")
	})
	if err != nil {
		reprocessAd = true
		log.Errorw("Failed to sync", "err", err)
		return
	}

	elapsed := time.Since(startTime)
	// Record how long sync took.
	stats.Record(context.Background(), metrics.EntriesSyncLatency.M(coremetrics.MsecSince(startTime)))
	log.Infow("Finished syncing entries", "elapsed", elapsed)

	ing.signalMetricsUpdate()
}

func (ing *Ingester) addSkip(key string) {
	ing.skipsMutex.Lock()
	defer ing.skipsMutex.Unlock()

	if ing.skips == nil {
		ing.skips = make(map[string]struct{})
	}
	ing.skips[key] = struct{}{}
}

func (ing *Ingester) hasSkip(key string) bool {
	ing.skipsMutex.Lock()
	defer ing.skipsMutex.Unlock()

	if len(ing.skips) == 0 {
		return false
	}
	_, skip := ing.skips[key]
	return skip
}

func (ing *Ingester) delSkip(key string) {
	ing.skipsMutex.Lock()
	defer ing.skipsMutex.Unlock()

	delete(ing.skips, key)
	if len(ing.skips) == 0 {
		ing.skips = nil
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
		return fmt.Errorf("cannot decode entries: %w", err)
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
			return fmt.Errorf("cannot decode a multihash from content block: %w", err)
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
			return fmt.Errorf("cannot remove multihashes from indexer: %w", err)
		}
		return fmt.Errorf("cannot put multihashes into indexer: %w", err)
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
		return indexer.Value{}, false, fmt.Errorf("cannot read advertisement for entry from datastore: %w", err)
	}
	// Decode the advertisement.
	adn, err := decodeIPLDNode(adCid.Prefix().Codec, bytes.NewBuffer(adb))
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decode ipld node: %w", err)
	}
	ad, err := decodeAd(adn)
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decode advertisement: %w", err)
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
		return indexer.Value{}, false, fmt.Errorf("cannot decode metadata: %w", err)
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
		return cid.Undef, fmt.Errorf("cannot load advertisement cid for entries cid from datastore: %w", err)
	}

	n, adCid, err := cid.CidFromBytes(data)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot decode advertisement cid: %w", err)
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
