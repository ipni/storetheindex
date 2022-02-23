package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
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
			} else {
				log.Debug("Received IPLD node")
			}
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
	provID, err := peer.Decode(ad.Provider.String())
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

// ingestEntryChunk determines the logic to run when a new block is received through
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
func (ing *Ingester) ingestEntryChunk(publisher peer.ID, adCid cid.Cid, entryChunkCid cid.Cid) error {
	log := log.With("publisher", publisher, "adCid", adCid, "cid", entryChunkCid)

	// Get data corresponding to the block.
	entryChunkKey := dsKey(entryChunkCid.String())
	val, err := ing.ds.Get(context.Background(), entryChunkKey)
	if err != nil {
		return fmt.Errorf("cannot fetch the node from datastore: %w", err)
	}
	defer func() {
		// Remove the content block from the data store now that processing it
		// has finished.  This prevents storing redundant information in
		// several datastores.
		err := ing.ds.Delete(context.Background(), entryChunkKey)
		if err != nil {
			log.Errorw("Error deleting index from datastore", "err", err)
		}
	}()

	// Decode block to IPLD node
	node, err := decodeIPLDNode(entryChunkCid.Prefix().Codec, bytes.NewBuffer(val))
	if err != nil {
		return fmt.Errorf("failed to decode ipldNode: %w", err)
	}

	err = ing.indexContentBlock(adCid, publisher, node)
	if err != nil {
		return fmt.Errorf("failed processing entries for advertisement: %w", err)
	} else {
		log.Info("Done indexing content in entry block")
		ing.signalMetricsUpdate()
	}

	return nil
}

// ingestAd fetches all the entries for a single advertisement
func (ing *Ingester) ingestAd(publisher peer.ID, adCid cid.Cid) error {
	stats.Record(context.Background(), metrics.IngestChange.M(1))
	var skip bool
	ingestStart := time.Now()
	defer func() {
		stats.Record(context.Background(), metrics.AdIngestLatency.M(coremetrics.MsecSince(ingestStart)))
	}()

	log := log.With("publisher", publisher, "adCid", adCid)

	log.Info("Processing advertisement")

	ad, err := ing.loadAd(adCid, true)
	if err != nil {
		return fmt.Errorf("failed to load ad: %v", err)
	}

	// Get provider ID from advertisement.
	providerID, err := peer.Decode(ad.Provider.String())
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

	// Mark the ad as processed after done processing. This is even in most
	// error cases so that the indexer is not stuck trying to reprocessing a
	// malformed ad.
	var reprocessAd bool
	defer func() {
		if !reprocessAd {
			// Processed all the entries, so mark this ad as processed.
			if err := ing.markAdProcessed(publisher, adCid); err != nil {
				log.Errorw("Failed to mark ad as processed", "err", err)
			} else {
				stats.Record(context.Background(), metrics.AdSyncedCount.M(1))
			}
		}
		// Distribute the legs.SyncFinished notices to waiting Sync calls.
		ing.inEvents <- legs.SyncFinished{Cid: adCid, PeerID: publisher}
	}()

	// This ad has bad data or has all of its content deleted by a subsequent
	// ad, so skip any further processing.
	if skip {
		return err
	}

	addrs, err := schema.IpldToGoStrings(ad.FieldAddresses())
	if err != nil {
		return fmt.Errorf("could not get addresses from advertisement: %w", err)
	}

	// Register provider or update existing registration.  The
	// provider must be allowed by policy to be registered.
	err = ing.reg.RegisterOrUpdate(context.Background(), providerID, addrs, adCid)
	if err != nil {
		return fmt.Errorf("could not register/update provider info: %w", err)
	}

	log = log.With("contextID", base64.StdEncoding.EncodeToString(contextID), "provider", providerID)

	if isRm {
		log.Infow("Advertisement is for removal by context id")

		err = ing.indexer.RemoveProviderContext(providerID, contextID)
		if err != nil {
			return fmt.Errorf("failed to removed content by context ID: %w", err)
		}
		return nil
	}

	// If advertisement has no entries, then this is for updating metadata only.
	if elink == schema.NoEntries {
		// If this is a metadata update only, then ad will not have entries.
		metadataBytes, err := ad.FieldMetadata().AsBytes()
		if err != nil {
			log.Errorw("Error reading advertisement metadata", "err", err)
			return fmt.Errorf("error reading advertisement metadata: %w", err)
		}

		value := indexer.Value{
			ContextID:     contextID,
			MetadataBytes: metadataBytes,
			ProviderID:    providerID,
		}

		log.Error("Advertisement is metadata update only")
		ing.indexer.Put(value)
		return nil
	}

	entriesCid := elink.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		log.Error("Advertisement entries link is undefined")
		return fmt.Errorf("advertisement entries link is undefined")
	}
	log = log.With("entriesCid", entriesCid)

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
	var errsIngestingEntryChunks []error
	_, err = ing.sub.SyncWithHook(ctx, publisher, entriesCid, ing.entriesSel, nil, func(p peer.ID, c cid.Cid) {
		err := ing.ingestEntryChunk(p, adCid, c)
		if err != nil {
			errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
		}
	})
	if err != nil {
		reprocessAd = true
		return fmt.Errorf("failed to sync: %w", err)
	}

	elapsed := time.Since(startTime)
	// Record how long sync took.
	stats.Record(context.Background(), metrics.EntriesSyncLatency.M(coremetrics.MsecSince(startTime)))
	log.Infow("Finished syncing entries", "elapsed", elapsed)

	ing.signalMetricsUpdate()

	if len(errsIngestingEntryChunks) > 0 {
		return fmt.Errorf("errors while ingesting entry chunks: %v", errsIngestingEntryChunks)
	}
	return nil
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

	// Load the advertisement data for this chunk.  If there are more chunks to
	// follow, then cache the ad data.
	hasNextLink := nchunk.Next.IsAbsent() || nchunk.Next.IsNull()
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

func (ing *Ingester) loadAd(adCid cid.Cid, keepCache bool) (ad schema.Advertisement, err error) {
	ing.adCacheMutex.Lock()
	ad, ok := ing.adCache[adCid]
	if !keepCache && ok {
		if len(ing.adCache) == 1 {
			ing.adCache = nil
		} else {
			delete(ing.adCache, adCid)
		}
	}
	ing.adCacheMutex.Unlock()

	defer func() {
		if keepCache {
			ing.adCacheMutex.Lock()
			if ing.adCache == nil {
				ing.adCache = make(map[cid.Cid]schema.Advertisement)
			}
			ing.adCache[adCid] = ad
			ing.adCacheMutex.Unlock()
		}
	}()

	if ok {
		return ad, nil
	}

	// Getting the advertisement for the entries so we know
	// what metadata and related information we need to use for ingestion.
	adb, err := ing.ds.Get(context.Background(), dsKey(adCid.String()))
	if err != nil {
		return nil, fmt.Errorf("cannot read advertisement for entry from datastore: %w", err)
	}

	// Decode the advertisement.
	adn, err := decodeIPLDNode(adCid.Prefix().Codec, bytes.NewBuffer(adb))
	if err != nil {
		return nil, fmt.Errorf("cannot decode ipld node: %w", err)
	}
	ad, err = decodeAd(adn)
	if err != nil {
		return nil, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	return ad, nil
}

func (ing *Ingester) loadAdData(adCid cid.Cid, keepCache bool) (value indexer.Value, isRm bool, err error) {
	ad, err := ing.loadAd(adCid, keepCache)
	if err != nil {
		return indexer.Value{}, false, err
	}
	providerID, err := peer.Decode(ad.Provider.String())
	if err != nil {
		return indexer.Value{}, false, err
	}

	value = indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID.Bytes(),
		MetadataBytes: ad.Metadata.Bytes(),
	}

	return value, ad.IsRm.Bool(), nil
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
