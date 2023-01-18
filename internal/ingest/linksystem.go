package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/api/v0/ingest/schema"
	"github.com/ipni/storetheindex/dagsync"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.uber.org/zap"

	// Import so these codecs get registered.
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

var (
	errBadAdvert              = errors.New("bad advertisement")
	errInvalidAdvertSignature = errors.New("invalid advertisement signature")
)

// mkLinkSystem makes the indexer linkSystem which checks advertisement
// signatures at storage. If the signature is not valid the traversal/exchange
// is terminated.
func mkLinkSystem(ds datastore.Batching, reg *registry.Registry) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
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
			return ds.Put(lctx.Ctx, datastore.NewKey(c.String()), origBuf)
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

	if err = ad.Validate(); err != nil {
		log.Errorw("Advertisement validation failed", "err", err)
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
//
// The publisherID is the peer ID of the message publisher. This is not necessarily
// the same as the provider ID in the advertisement. The publisher is the
// source of the indexed content, the provider is where content can be
// retrieved from. It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) ingestAd(publisherID peer.ID, adCid cid.Cid, ad schema.Advertisement, resync, frozen bool) error {
	stats.Record(context.Background(), metrics.IngestChange.M(1))
	var mhCount int
	var entsSyncStart time.Time
	var entsStoreElapsed time.Duration
	ingestStart := time.Now()

	log := log.With("publisher", publisherID, "adCid", adCid)

	defer func() {
		now := time.Now()

		// Record how long ad sync took.
		elapsed := now.Sub(ingestStart)
		elapsedMsec := float64(elapsed.Nanoseconds()) / 1e6
		stats.Record(context.Background(), metrics.AdIngestLatency.M(elapsedMsec))
		log.Infow("Finished syncing advertisement", "elapsed", elapsed.String(), "multihashes", mhCount)

		if mhCount == 0 {
			return
		}

		// Record how long entries sync took.
		elapsed = now.Sub(entsSyncStart)
		elapsedMsec = float64(elapsed.Nanoseconds()) / 1e6
		stats.Record(context.Background(), metrics.EntriesSyncLatency.M(elapsedMsec))

		// Record average time to store one multihash, for all multihahses in
		// this ad's entries.
		elapsedPerMh := int64(math.Round(float64(entsStoreElapsed.Nanoseconds()) / float64(mhCount)))
		stats.Record(context.Background(), metrics.MhStoreNanoseconds.M(elapsedPerMh))
	}()

	// Get provider ID from advertisement.
	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return adIngestError{adIngestDecodingErr, fmt.Errorf("failed to read provider id: %w", err)}
	}

	// Register provider or update existing registration. The provider must be
	// allowed by policy to be registered.
	var publisher peer.AddrInfo
	peerStore := ing.sub.HttpPeerStore()
	if peerStore != nil {
		publisher = peerStore.PeerInfo(publisherID)
	}
	if len(publisher.Addrs) == 0 {
		peerStore = ing.host.Peerstore()
		if peerStore != nil {
			publisher = peerStore.PeerInfo(publisherID)
		}
	}

	maddrs := stringsToMultiaddrs(ad.Addresses)

	provider := peer.AddrInfo{
		ID:    providerID,
		Addrs: maddrs,
	}

	ctx := context.Background()

	var extendedProviders *registry.ExtendedProviders
	if ad.ExtendedProvider != nil {
		if ad.IsRm {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("rm ads can not have extended providers")}
		}

		if len(ad.ContextID) == 0 && ad.ExtendedProvider.Override {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("override can not be set on extended provider without context id")}
		}

		// Fetching the existing ExtendedProvider record or creating a new one
		existingPInfo, _ := ing.reg.ProviderInfo(providerID)
		if existingPInfo != nil {
			extendedProviders = existingPInfo.ExtendedProviders
		}

		if extendedProviders == nil {
			extendedProviders = &registry.ExtendedProviders{
				ContextualProviders: make(map[string]registry.ContextualExtendedProviders),
			}
		}

		// Creating ExtendedProviderInfo record for each of the providers.
		// Keeping in mind that the provider from the outer ad doesn't need to be included into the extended providers list
		eProvs := make([]registry.ExtendedProviderInfo, 0, len(ad.ExtendedProvider.Providers))
		for _, ep := range ad.ExtendedProvider.Providers {
			epID, err := peer.Decode(ep.ID)
			if err != nil {
				return adIngestError{adIngestRegisterProviderErr, fmt.Errorf("could not register/update extended provider info: %w", err)}
			}

			eProvs = append(eProvs, registry.ExtendedProviderInfo{
				PeerID:   epID,
				Metadata: ep.Metadata,
				Addrs:    stringsToMultiaddrs(ep.Addresses),
			})
		}

		// If context ID is empty then it's a chain level record, otherwise it's specific to the context ID
		if len(ad.ContextID) == 0 {
			extendedProviders.Providers = eProvs
		} else {
			extendedProviders.ContextualProviders[string(ad.ContextID)] = registry.ContextualExtendedProviders{
				ContextID: ad.ContextID,
				Override:  ad.ExtendedProvider.Override,
				Providers: eProvs,
			}
		}
	}

	err = ing.reg.Update(ctx, provider, publisher, adCid, extendedProviders)
	if err != nil {
		return adIngestError{adIngestRegisterProviderErr, fmt.Errorf("could not register/update provider info: %w", err)}
	}

	log = log.With("contextID", base64.StdEncoding.EncodeToString(ad.ContextID), "provider", providerID)

	if ad.IsRm {
		log.Infow("Advertisement is for removal by context id")

		err = ing.indexer.RemoveProviderContext(providerID, ad.ContextID)
		if err != nil {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to remove provider context: %w", err)}
		}
		if ing.indexCounts != nil {
			rmCount, err := ing.indexCounts.RemoveCtx(providerID, ad.ContextID)
			if err != nil {
				log.Errorw("Error removing index count", "err", err)
			} else {
				log.Debugf("Removal ad reduced index count by %d", rmCount)
			}
		}
		return nil
	}

	if len(ad.Metadata) == 0 {
		// If the ad has no metadata and no entries, then the ad is only for
		// updating provider addresses. Otherwise it is an error.
		if ad.Entries != schema.NoEntries {
			return adIngestError{adIngestMalformedErr, fmt.Errorf("advertisement missing metadata")}
		}
		return nil
	}

	// If advertisement has no entries, then it is for updating metadata only.
	if ad.Entries == schema.NoEntries || frozen {
		// If this is a metadata update only, then ad will not have entries.
		value := indexer.Value{
			ContextID:     ad.ContextID,
			MetadataBytes: ad.Metadata,
			ProviderID:    providerID,
		}

		if frozen {
			log.Infow("Indexer frozen, advertisement only updates metadata")
		} else {
			log.Infow("Advertisement is metadata update only")
		}
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

	if ing.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer cancel()
	}

	entsSyncStart = time.Now()

	// The ad.Entries link can point to either a chain of EntryChunks or a
	// HAMT. Sync the very first entry so that we can check which type it is.
	// This means the maximum depth of entries traversal will be 1 plus the
	// configured max depth.
	//
	// TODO: See if it is worth detecting and reducing depth the depth in
	// entries selectors by one.
	syncedFirstEntryCid, err := ing.sub.Sync(ctx, publisherID, entriesCid, Selectors.One, nil)
	if err != nil {
		// TODO: A "content not found" error from graphsync does not have a
		// graphsync.RequestFailedContentNotFoundErr in the error chain. Need
		// to apply an upstream fix so that the following can be done:
		//
		//   var cnfErr *graphsync.RequestFailedContentNotFoundErr
		//   if errors.As(err, &cnfErr) {
		//
		// Use string search until then.
		wrappedErr := fmt.Errorf("failed to sync first entry while checking entries type: %w", err)
		msg := err.Error()
		switch {
		case
			strings.Contains(msg, "content not found"),
			strings.Contains(msg, "graphsync request failed to complete: skip"):
			return adIngestError{adIngestContentNotFound, wrappedErr}
		default:
			return adIngestError{adIngestSyncEntriesErr, wrappedErr}
		}
	}

	node, err := ing.loadNode(syncedFirstEntryCid, basicnode.Prototype.Any)
	if err != nil {
		return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to load first entry after sync: %w", err)}
	}

	var errsIngestingEntryChunks []error
	if isHAMT(node) {
		log = log.With("entriesKind", "hamt")
		// Keep track of all CIDs in the HAMT to remove them later when the
		// processing is done. This is equivalent behaviour to ingestEntryChunk
		// which removes an entry chunk right afrer it is processed.
		hamtCids := []cid.Cid{syncedFirstEntryCid}
		gatherCids := func(_ peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
			hamtCids = append(hamtCids, c)
		}
		defer func() {
			for _, c := range hamtCids {
				err := ing.dsAds.Delete(ctx, datastore.NewKey(c.String()))
				if err != nil {
					log.Errorw("Error deleting HAMT cid from datastore", "cid", c, "err", err)
				}
			}
		}()

		// Load the CID as HAMT root node.
		hn, err := ing.loadHamt(syncedFirstEntryCid)
		if err != nil {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to load entries as HAMT root node: %w", err)}
		}

		// Sync all the links in the hamt, since so far we have only synced the root.
		for _, e := range hn.Hamt.Data {
			if e.HashMapNode != nil {
				nodeCid := (*e.HashMapNode).(cidlink.Link).Cid
				_, err = ing.sub.Sync(ctx, publisherID, nodeCid, Selectors.All, nil,
					// Gather all the HAMT Cids so that we can remove them from
					// datastore once finished processing.
					dagsync.ScopedBlockHook(gatherCids),
					// Disable segmented sync.
					//
					// TODO: see if segmented sync for HAMT makes sense and if
					// so modify block hook action above appropriately.
					dagsync.ScopedSegmentDepthLimit(-1))
				if err != nil {
					wrappedErr := fmt.Errorf("failed to sync remaining HAMT: %w", err)
					if strings.Contains(err.Error(), "content not found") {
						return adIngestError{adIngestContentNotFound, wrappedErr}
					}
					return adIngestError{adIngestSyncEntriesErr, wrappedErr}
				}
			}
		}

		entsStoreStart := time.Now()

		// Start processing now that the entire HAMT is synced. HAMT is a map,
		// and we are using the keys in the map to represent multihashes.
		// Therefore, we only care about the keys.
		//
		// Group the mutlihashes in StoreBatchSize batches and process as usual.
		mhs := make([]multihash.Multihash, 0, ing.batchSize)
		mi := hn.MapIterator()
		for !mi.Done() {
			k, _, err := mi.Next()
			if err != nil {
				return adIngestError{adIngestIndexerErr, fmt.Errorf("faild to iterate through HAMT: %w", err)}
			}
			ks, err := k.AsString()
			if err != nil {
				return adIngestError{adIngestMalformedErr, fmt.Errorf("HAMT key must be of type string: %w", err)}
			}
			mhs = append(mhs, multihash.Multihash(ks))
			// The reason we need batching here is because here we are
			// iterating over the _entire_ HAMT keys, whereas indexContentBlock
			// is meant to be given multihashes in a single EntryChunk which
			// could be far fewer multihashes.
			//
			// Batching here allows us to only load into memory one batch worth
			// of multihashes from the HAMT, instead of loading all the
			// multihashes in the HAMT then batch them later in
			// indexContentBlock.
			//
			// TODO: See how we can refactor code to make batching logic more
			// flexible in indexContentBlock.
			if len(mhs) >= int(ing.batchSize) {
				if err = ing.indexAdMultihashes(ad, mhs, log); err != nil {
					return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
				}
				mhCount += len(mhs)
				mhs = mhs[:0]
			}
		}
		// Process any remaining multihashes from the batch cut-off.
		if len(mhs) > 0 {
			if err = ing.indexAdMultihashes(ad, mhs, log); err != nil {
				return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
			}
			mhCount += len(mhs)
		}
		entsStoreElapsed = time.Since(entsStoreStart)
	} else {
		log = log.With("entriesKind", "EntryChunk")

		// We have already peeked at the first EntryChunk as part of probing
		// the entries type, so process that first.
		chunk, err := ing.loadEntryChunk(syncedFirstEntryCid)
		if err != nil {
			errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
		} else {
			chunkStart := time.Now()
			err = ing.ingestEntryChunk(ctx, ad, syncedFirstEntryCid, *chunk, log)
			if err != nil {
				errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
			}
			mhCount += len(chunk.Entries)
			entsStoreElapsed += time.Since(chunkStart)
		}

		if chunk != nil && chunk.Next != nil {
			nextChunkCid := chunk.Next.(cidlink.Link).Cid
			// Traverse remaining entry chunks based on the entries selector
			// that limits recursion depth.
			_, err = ing.sub.Sync(ctx, publisherID, nextChunkCid, ing.entriesSel, nil, dagsync.ScopedBlockHook(func(p peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
				chunkStart := time.Now()

				// Load CID as entry chunk since the selector should only
				// select entry chunk nodes.
				chunk, err := ing.loadEntryChunk(c)
				if err != nil {
					actions.FailSync(err)
					errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
					return
				}
				err = ing.ingestEntryChunk(ctx, ad, c, *chunk, log)
				if err != nil {
					actions.FailSync(err)
					errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
					return
				}
				mhCount += len(chunk.Entries)
				entsStoreElapsed += time.Since(chunkStart)

				if chunk.Next != nil {
					actions.SetNextSyncCid(chunk.Next.(cidlink.Link).Cid)
				} else {
					actions.SetNextSyncCid(cid.Undef)
				}
			}))
			if err != nil {
				wrappedErr := fmt.Errorf("failed to sync entries: %w", err)
				if strings.Contains(err.Error(), "content not found") {
					return adIngestError{adIngestContentNotFound, wrappedErr}
				}
				return adIngestError{adIngestSyncEntriesErr, wrappedErr}
			}
		}
	}
	if ing.indexCounts != nil {
		if resync {
			// If resyncing, only add missing values so that counts are not duplicated.
			ing.indexCounts.AddMissingCount(providerID, ad.ContextID, uint64(mhCount))
		} else {
			ing.indexCounts.AddCount(providerID, ad.ContextID, uint64(mhCount))
		}
	}

	if len(errsIngestingEntryChunks) > 0 {
		return adIngestError{adIngestEntryChunkErr, fmt.Errorf("failed to ingest entry chunks: %v", errsIngestingEntryChunks)}
	}
	return nil
}

// ingestEntryChunk ingests a block of entries as that block is received
// through graphsync.
//
// When each advertisement on a chain is processed by ingestAd, that
// advertisement's entries are synced in a separate dagsync.Subscriber.Sync
// operation. This function is used as a scoped block hook, and is called for
// each block that is received.
func (ing *Ingester) ingestEntryChunk(ctx context.Context, ad schema.Advertisement, entryChunkCid cid.Cid, chunk schema.EntryChunk, log *zap.SugaredLogger) error {
	defer func() {
		// Remove the content block from the data store now that processing it
		// has finished. This prevents storing redundant information in several
		// datastores.
		entryChunkKey := datastore.NewKey(entryChunkCid.String())
		err := ing.dsAds.Delete(ctx, entryChunkKey)
		if err != nil {
			log.Errorw("Error deleting index from datastore", "err", err)
		}
	}()

	err := ing.indexAdMultihashes(ad, chunk.Entries, log)
	if err != nil {
		return fmt.Errorf("failed processing entries for advertisement: %w", err)
	}

	return nil
}

// indexAdMultihashes indexes filters out invalid multihashed and indexes those
// remaining in the indexer core.
func (ing *Ingester) indexAdMultihashes(ad schema.Advertisement, mhs []multihash.Multihash, log *zap.SugaredLogger) error {
	// Build indexer.Value from ad data.
	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return err
	}
	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	// Iterate over multihashes and remove bad ones.
	var badMultihashCount int
	for i := 0; i < len(mhs); {
		var badMultihash bool
		decoded, err := multihash.Decode(mhs[i])
		if err != nil {
			// Only log first error to prevent log flooding.
			if badMultihashCount == 0 {
				log.Warnw("Ignoring bad multihash", "err", err)
			}
			badMultihash = true
		} else if len(decoded.Digest) < ing.minKeyLen {
			log.Warnw("Multihash digest too short, ignoring", "digestSize", len(decoded.Digest))
			badMultihash = true
		}
		if badMultihash {
			// Remove the bad multihash.
			mhs[i] = mhs[len(mhs)-1]
			mhs[len(mhs)-1] = nil
			mhs = mhs[:len(mhs)-1]
			badMultihashCount++
			continue
		}
		i++
	}
	if badMultihashCount != 0 {
		log.Warnw("Ignored bad multihashes", "ignored", badMultihashCount)
	}
	if len(mhs) == 0 {
		return nil
	}

	// No code path should ever allow this, so it is a programming error if
	// this ever happens.
	if ad.IsRm {
		panic("removing individual multihashes no allowed")
	}

	if err = ing.indexer.Put(value, mhs...); err != nil {
		return fmt.Errorf("cannot put multihashes into indexer: %w", err)
	}
	log.Infow("Put multihashes in entry chunk", "count", len(mhs))

	return nil
}

func (ing *Ingester) loadAd(c cid.Cid) (schema.Advertisement, error) {
	adn, err := ing.loadNode(c, schema.AdvertisementPrototype)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot decode ipld node: %w", err)
	}
	ad, err := schema.UnwrapAdvertisement(adn)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	return *ad, nil
}

func (ing *Ingester) loadEntryChunk(c cid.Cid) (*schema.EntryChunk, error) {
	node, err := ing.loadNode(c, schema.EntryChunkPrototype)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ipldNode: %w", err)
	}
	return schema.UnwrapEntryChunk(node)
}

func (ing *Ingester) loadHamt(c cid.Cid) (*hamt.Node, error) {
	node, err := ing.loadNode(c, hamt.HashMapRootPrototype)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ipldNode: %w", err)
	}
	root := bindnode.Unwrap(node).(*hamt.HashMapRoot)
	if root == nil {
		return nil, errors.New("cannot unwrap node as hamt.HashMapRoot")
	}
	hn := hamt.Node{
		HashMapRoot: *root,
	}.WithLinking(ing.lsys, schema.Linkproto)
	return hn, nil
}

func (ing *Ingester) loadNode(c cid.Cid, prototype ipld.NodePrototype) (ipld.Node, error) {
	key := datastore.NewKey(c.String())
	val, err := ing.dsAds.Get(context.Background(), key)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch the node from datastore: %w", err)
	}
	return decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), prototype)
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

// isAdvertisement checks if an IPLD node is an advertisement, by looking to see if it has a
// "Signature" field. Additional checks may be needed if the schema is extended
// with new types that are traversable.
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}

// isHAMT checks if the given IPLD node is a HAMT root node by looking for a field named  "hamt".
//
// See: https://github.com/ipld/go-ipld-adl-hamt/blob/master/schema.ipldsch
func isHAMT(n ipld.Node) bool {
	h, _ := n.LookupByString("hamt")
	return h != nil
}

func stringsToMultiaddrs(addrs []string) []multiaddr.Multiaddr {
	var maddrs []multiaddr.Multiaddr
	if len(addrs) != 0 {
		maddrs = make([]multiaddr.Multiaddr, 0, len(addrs))
		for _, addr := range addrs {
			maddr, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				log.Warnw("Bad address in advertisement", "address", addr)
				continue
			}
			maddrs = append(maddrs, maddr)
		}
	}
	return maddrs
}
