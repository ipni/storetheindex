package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
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
			codec := lnk.(cidlink.Link).Prefix().Codec
			origBuf := buf.Bytes()

			log := log.With("cid", c)

			// Decode the node to check its type.
			n, err := decodeIPLDNode(codec, buf, basicnode.Prototype.Any)
			if err != nil {
				log.Errorw("Error decoding IPLD node in linksystem", "err", err)
				return errors.New("bad ipld data")
			}
			// If it is an advertisement.
			if isAdvertisement(n) {
				// Verify that the signature is correct and the advertisement
				// is valid.
				provID, err := verifyAdvertisement(n, reg)
				if err != nil {
					return err
				}

				log.Infow("Received advertisement", "provider", provID)
			} else {
				log.Debug("Received IPLD node")
			}
			// Any other type of node (like entries) are stored right away.
			return ds.Put(lctx.Ctx, dsKey(c.String()), origBuf)
		}, nil
	}
	return lsys
}

func verifyAdvertisement(n ipld.Node, reg *registry.Registry) (peer.ID, error) {
	ad, err := schema.UnwrapAdvertisement(n)
	if err != nil {
		log.Errorw("Cannot decode advertisement", "err", err)
		return "", errBadAdvert
	}
	// Verify advertisement signature.
	signerID, err := ad.VerifySignature()
	if err != nil {
		// stop exchange, verification of signature failed.
		log.Errorw("Advertisement signature verification failed", "err", err)
		return "", errInvalidAdvertSignature
	}

	// Get provider ID from advertisement.
	provID, err := peer.Decode(ad.Provider)
	if err != nil {
		log.Errorw("Cannot get provider from advertisement", "err", err, "signer", signerID)
		return "", errBadAdvert
	}

	// Verify that the advertisement is signed by the provider or by an allowed
	// publisher.
	if signerID != provID && !reg.PublishAllowed(signerID, provID) {
		log.Errorw("Advertisement not signed by provider or allowed publisher", "provider", ad.Provider, "signer", signerID)
		return "", errInvalidAdvertSignature
	}

	return provID, nil
}

// ingestAd fetches all the entries for a single advertisement and processes
// them. This is called for each advertisement in a synced chain by an ingester
// worker. The worker begins processing the synced advertisement chain when it
// receives notification that a peer has finished a sync for advertisements.
//
// Advertisements are processed from oldest to newest, which is the reverse
// order that they were received in.
func (ing *Ingester) ingestAd(publisherID peer.ID, adCid cid.Cid, ad schema.Advertisement) error {
	stats.Record(context.Background(), metrics.IngestChange.M(1))
	ingestStart := time.Now()
	defer func() {
		stats.Record(context.Background(), metrics.AdIngestLatency.M(coremetrics.MsecSince(ingestStart)))
	}()

	log := log.With("publisher", publisherID, "adCid", adCid)

	// Get provider ID from advertisement.
	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return adIngestError{adIngestDecodingErr, fmt.Errorf("failed to read provider id: %w", err)}
	}

	// Register provider or update existing registration. The provider must be
	// allowed by policy to be registered.
	var pubInfo peer.AddrInfo
	peerStore := ing.sub.HttpPeerStore()
	if peerStore != nil {
		pubInfo = peerStore.PeerInfo(publisherID)
	}
	if len(pubInfo.Addrs) == 0 {
		peerStore = ing.host.Peerstore()
		if peerStore != nil {
			pubInfo = peerStore.PeerInfo(publisherID)
		}
	}
	err = ing.reg.RegisterOrUpdate(context.Background(), providerID, ad.Addresses, adCid, pubInfo)
	if err != nil {
		return adIngestError{adIngestRegisterProviderErr, fmt.Errorf("could not register/update provider info: %w", err)}
	}

	log = log.With("contextID", base64.StdEncoding.EncodeToString(ad.ContextID), "provider", ad.Provider)

	if ad.IsRm {
		log.Infow("Advertisement is for removal by context id")

		err = ing.indexer.RemoveProviderContext(providerID, ad.ContextID)
		if err != nil {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to remove provider context: %w", err)}
		}
		return nil
	}

	// If advertisement has no entries, then this is for updating metadata only.
	if ad.Entries == schema.NoEntries {
		// If this is a metadata update only, then ad will not have entries.
		value := indexer.Value{
			ContextID:     ad.ContextID,
			MetadataBytes: ad.Metadata,
			ProviderID:    providerID,
		}

		log.Error("Advertisement is metadata update only")
		err = ing.indexer.Put(value)
		if err != nil {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to update metadata: %w", err)}
		}
		return nil
	}

	entriesCid := ad.Entries.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		return adIngestError{adIngestMalformedErr, fmt.Errorf("advertisement entries link is undefined")}
	}
	log = log.With("entriesCid", entriesCid)

	ctx := context.Background()
	if ing.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer cancel()
	}

	startTime := time.Now()

	// Traverse entries based on the entries selector that limits recursion depth.
	var errsIngestingEntryChunks []error
	_, err = ing.sub.Sync(ctx, publisherID, entriesCid, ing.entriesSel, nil, legs.ScopedBlockHook(func(p peer.ID, c cid.Cid, actions legs.SegmentSyncActions) {
		// Load CID as entry chunk since the selector should only select entry chunk nodes.
		chunk, err := ing.loadEntryChunk(c)
		if err != nil {
			actions.FailSync(err)
			errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
			return
		}
		err = ing.ingestEntryChunk(ctx, p, adCid, ad, c, *chunk)
		if err != nil {
			actions.FailSync(err)
			errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
			return
		}
		if chunk.Next != nil {
			actions.SetNextSyncCid((*(chunk.Next)).(cidlink.Link).Cid)
		}
	}))
	if err != nil {
		if strings.Contains(err.Error(), "datatransfer failed: content not found") {
			return adIngestError{adIngestContentNotFound, fmt.Errorf("failed to sync entries: %w", err)}
		}
		return adIngestError{adIngestSyncEntriesErr, fmt.Errorf("failed to sync entries: %w", err)}
	}

	elapsed := time.Since(startTime)
	// Record how long sync took.
	stats.Record(context.Background(), metrics.EntriesSyncLatency.M(coremetrics.MsecSince(startTime)))
	log.Infow("Finished syncing entries", "elapsed", elapsed)

	ing.signalMetricsUpdate()

	if len(errsIngestingEntryChunks) > 0 {
		return adIngestError{adIngestEntryChunkErr, fmt.Errorf("failed to ingest entry chunks: %v", errsIngestingEntryChunks)}
	}
	return nil
}

// ingestEntryChunk ingests a block of entries as that block is received
// through graphsync.
//
// When each advertisement on a chain is processed by ingestAd, that
// advertisement's entries are synced in a spearate legs.Subscriber.Sync
// operation. This function is used as a scoped block hook, and is called for
// each block that is received.
func (ing *Ingester) ingestEntryChunk(ctx context.Context, publisher peer.ID, adCid cid.Cid, ad schema.Advertisement, entryChunkCid cid.Cid, chunk schema.EntryChunk) error {
	log := log.With("publisher", publisher, "adCid", adCid, "cid", entryChunkCid)

	defer func() {
		// Remove the content block from the data store now that processing it
		// has finished. This prevents storing redundant information in several
		// datastores.
		entryChunkKey := dsKey(entryChunkCid.String())
		err := ing.ds.Delete(ctx, entryChunkKey)
		if err != nil {
			log.Errorw("Error deleting index from datastore", "err", err)
		}
	}()

	err := ing.indexContentBlock(adCid, ad, publisher, chunk)
	if err != nil {
		return fmt.Errorf("failed processing entries for advertisement: %w", err)
	}

	ing.signalMetricsUpdate()
	return nil
}

// indexContentBlock indexes the content multihashes in a block of data. First
// the advertisement is loaded to get the context ID and metadata. Then the
// metadata and multihashes in the content block are indexed by the
// indexer-core.
//
// The pubID is the peer ID of the message publisher. This is not necessarily
// the same as the provider ID in the advertisement. The publisher is the
// source of the indexed content, the provider is where content can be
// retrieved from. It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) indexContentBlock(adCid cid.Cid, ad schema.Advertisement, pubID peer.ID, nchunk schema.EntryChunk) error {
	log := log.With("publisher", pubID, "adCid", adCid)

	// Load the advertisement data for this chunk. If there are more chunks to
	// follow, then cache the ad data.
	value, isRm, err := getAdData(ad)
	if err != nil {
		return err
	}

	batchChan := make(chan []multihash.Multihash)
	errChan := make(chan error, 1)
	// Start a goroutine that processes batches of multihashes.
	go func() {
		defer close(errChan)
		for mhs := range batchChan {
			if err := ing.storeBatch(value, mhs, isRm); err != nil {
				errChan <- err
				return
			}
		}
	}()

	batch := make([]multihash.Multihash, 0, ing.batchSize)
	var prevBatch []multihash.Multihash

	// Iterate over all entries and ingest (or remove) them.
	var count int
	for _, entry := range nchunk.Entries {
		batch = append(batch, entry)

		// Process full batch of multihashes.
		if len(batch) == cap(batch) {
			select {
			case batchChan <- batch:
			case err = <-errChan:
				return err
			}
			count += len(batch)
			if prevBatch == nil {
				prevBatch = make([]multihash.Multihash, 0, ing.batchSize)
			}
			// Since batchChan is unbuffered, the goroutine is done reading the previous batch.
			prevBatch, batch = batch, prevBatch
			batch = batch[:0]
		}
	}

	// Process any remaining multihashes.
	if len(batch) != 0 {
		select {
		case batchChan <- batch:
		case err = <-errChan:
			return err
		}
		count += len(batch)
	}

	close(batchChan)
	err = <-errChan
	if err != nil {
		return err
	}

	if isRm {
		log.Infow("Removed multihashes in entry chunk", "count", count)
	} else {
		log.Infow("Put multihashes in entry chunk", "count", count)
	}
	return nil
}

func (ing *Ingester) storeBatch(value indexer.Value, batch []multihash.Multihash, isRm bool) error {
	if isRm {
		if err := ing.indexer.Remove(value, batch...); err != nil {
			return fmt.Errorf("cannot remove multihashes from indexer: %w", err)
		}
	} else {
		if err := ing.indexer.Put(value, batch...); err != nil {
			return fmt.Errorf("cannot put multihashes into indexer: %w", err)
		}
	}
	return nil
}

func (ing *Ingester) loadAd(adCid cid.Cid) (schema.Advertisement, error) {
	adKey := dsKey(adCid.String())
	adb, err := ing.ds.Get(context.Background(), adKey)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot read advertisement for entry from datastore: %w", err)
	}

	// Decode the advertisement.
	adn, err := decodeIPLDNode(adCid.Prefix().Codec, bytes.NewBuffer(adb), schema.AdvertisementPrototype)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot decode ipld node: %w", err)
	}
	ad, err := schema.UnwrapAdvertisement(adn)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	return *ad, nil
}

func (ing *Ingester) loadEntryChunk(eCid cid.Cid) (*schema.EntryChunk, error) {
	entryChunkKey := dsKey(eCid.String())
	val, err := ing.ds.Get(context.Background(), entryChunkKey)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch the node from datastore: %w", err)
	}

	node, err := decodeIPLDNode(eCid.Prefix().Codec, bytes.NewBuffer(val), schema.EntryChunkPrototype)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ipldNode: %w", err)
	}
	return schema.UnwrapEntryChunk(node)
}

func getAdData(ad schema.Advertisement) (value indexer.Value, isRm bool, err error) {
	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return indexer.Value{}, false, err
	}

	value = indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	return value, ad.IsRm, nil
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(codec uint64, r io.Reader, prototype ipld.NodePrototype) (ipld.Node, error) {
	// NOTE: Considering using the schema prototypes. This was failing, using a
	// map gives flexibility. Maybe is worth revisiting this again in the
	// future.
	nb := prototype.NewBuilder()
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
// "Signature" field. Additional checks may be needed if the schema is extended
// with new types that are traversable.
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}
