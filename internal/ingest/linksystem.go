package ingest

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/mautil"
	"github.com/ipni/storetheindex/carstore"
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
				log.Debugw("Received advertisement", "provider", provID, "cid", c)
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
func (ing *Ingester) ingestAd(ctx context.Context, publisherID peer.ID, adCid cid.Cid, resync, frozen bool, lag int, headProvider peer.AddrInfo) error {
	log := log.With("publisher", publisherID, "adCid", adCid)

	ad, err := ing.loadAd(adCid)
	if err != nil {
		stats.Record(context.Background(), metrics.AdLoadError.M(1))
		log.Errorw("Failed to load advertisement, skipping", "err", err)
		// The ad cannot be loaded, so we cannot process it. Return nil so that
		// the ad is marked as processed and is removed from the datastore.
		return nil
	}

	stats.Record(ctx, metrics.IngestChange.M(1))
	var mhCount int
	var entsSyncStart time.Time
	ingestStart := time.Now()

	defer func() {
		now := time.Now()

		// Record how long ad sync took.
		elapsed := now.Sub(ingestStart)
		elapsedMsec := float64(elapsed.Nanoseconds()) / 1e6
		stats.Record(ctx, metrics.AdIngestLatency.M(elapsedMsec))
		log.Infow("Finished syncing advertisement", "elapsed", elapsed.String(), "multihashes", mhCount)

		if mhCount == 0 {
			return
		}

		// Record multihashes rate per provider.
		elapsed = now.Sub(entsSyncStart)
		ing.ingestRates.Update(string(headProvider.ID), uint64(mhCount), elapsed)

		// Record how long entries sync took.
		elapsedMsec = float64(elapsed.Nanoseconds()) / 1e6
		stats.Record(ctx, metrics.EntriesSyncLatency.M(elapsedMsec))
	}()

	// Since all advertisements in an assignment have the same provider,
	// the provider can be passed into ingestAd to avoid having to decode
	// the provider ID from each advertisement.
	providerID := headProvider.ID

	// Log provider ID if not the same as publisher ID.
	if providerID != publisherID {
		log = log.With("provider", providerID)
	}

	// Get publisher peer.AddrInfo from peerstore.
	publisher := peer.AddrInfo{
		ID: publisherID,
	}
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

	var extendedProviders *registry.ExtendedProviders
	if ad.ExtendedProvider != nil {
		if ad.IsRm {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("rm ads can not have extended providers")}
		}

		if len(ad.ContextID) == 0 && ad.ExtendedProvider.Override {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("override can not be set on extended provider without context id")}
		}

		extendedProviders = &registry.ExtendedProviders{
			ContextualProviders: make(map[string]registry.ContextualExtendedProviders),
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

	// If head ad does not have provider addresses, then update using addresses
	// from the current advertisement.
	if len(headProvider.Addrs) == 0 {
		headProvider.Addrs = stringsToMultiaddrs(ad.Addresses)
	}

	// Register provider or update existing registration. The provider must be
	// allowed by policy to be registered.
	err = ing.reg.Update(ctx, headProvider, publisher, adCid, extendedProviders, lag)
	if err != nil {
		// A registry.ErrMissingProviderAddr error is not considered a
		// permanent adIngestMalformedErr error, because an advertisement added
		// to the chain in the future may have a valid address than can be
		// used, allowing all the previous ads without valid addresses to be
		// processed.
		return adIngestError{adIngestRegisterProviderErr, fmt.Errorf("could not register/update provider info: %w", err)}
	}

	log = log.With("contextID", base64.StdEncoding.EncodeToString(ad.ContextID))

	if ad.IsRm {
		log.Infow("Advertisement is for removal by context id")

		err = ing.indexer.RemoveProviderContext(providerID, ad.ContextID)
		if err != nil {
			return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to remove provider context: %w", err)}
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
		return adIngestError{adIngestMalformedErr, errors.New("advertisement entries link is undefined")}
	}

	if ing.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer cancel()
	}

	entsSyncStart = time.Now()

	// If using a CAR reader, then try to get the advertisement CAR file first.
	if ing.mirror.canRead() {
		mhCount, err = ing.ingestEntriesFromCar(ctx, ad, providerID, adCid, entriesCid, log)
		// If entries data successfully read from CAR file.
		if err == nil {
			ing.mhsFromMirror.Add(uint64(mhCount))
			return nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			var adIngestErr adIngestError
			if errors.As(err, &adIngestErr) {
				switch adIngestErr.state {
				case adIngestIndexerErr:
					// Could not store multihashes in core, so stop trying to index ad.
					return err
				case adIngestContentNotFound:
					// No entries data in CAR file. Entries data deleted later
					// in chain unknown to this indexer, or publisher not
					// serving entries data.
					return err
				}
			}
			log.Errorw("Cannot get advertisement from car store", "err", err)
		}
	}

	// The ad.Entries link can point to either a chain of EntryChunks or a
	// HAMT. Sync the very first entry so that we can check which type it is.
	// This means the maximum depth of entries traversal will be 1 plus the
	// configured max depth.
	err = ing.sub.SyncOneEntry(ctx, publisher, entriesCid)
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
			errors.Is(err, ipld.ErrNotExists{}),
			strings.Contains(msg, "content not found"),
			strings.Contains(msg, "graphsync request failed to complete: skip"):
			return adIngestError{adIngestContentNotFound, wrappedErr}
		}
		return adIngestError{adIngestSyncEntriesErr, wrappedErr}
	}

	node, err := ing.loadNode(entriesCid, basicnode.Prototype.Any)
	if err != nil {
		return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to load first entry after sync: %w", err)}
	}

	if isHAMT(node) {
		mhCount, err = ing.ingestHamtFromPublisher(ctx, ad, publisherID, providerID, entriesCid, log)
	} else {
		mhCount, err = ing.ingestEntriesFromPublisher(ctx, ad, publisherID, providerID, entriesCid, log)
	}
	return err
}

func (ing *Ingester) ingestHamtFromPublisher(ctx context.Context, ad schema.Advertisement, publisherID, providerID peer.ID, entsCid cid.Cid, log *zap.SugaredLogger) (int, error) {
	// Split HAMP into batches of 4096 entries.
	const batchSize = 4096

	log = log.With("entriesKind", "hamt")
	// Keep track of all CIDs in the HAMT to remove them later when the
	// processing is done. This is equivalent behavior to ingestEntryChunk
	// which removes an entry chunk right afrer it is processed.
	hamtCids := []cid.Cid{entsCid}
	gatherCids := func(_ peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		hamtCids = append(hamtCids, c)
	}
	if !ing.mirror.canWrite() {
		defer func() {
			for _, c := range hamtCids {
				err := ing.dsTmp.Delete(ctx, datastore.NewKey(c.String()))
				if err != nil {
					log.Errorw("Error deleting HAMT cid from datastore", "cid", c, "err", err)
				}
			}
		}()
	}

	// Load the CID as HAMT root node.
	hn, err := ing.loadHamt(entsCid)
	if err != nil {
		return 0, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to load entries as HAMT root node: %w", err)}
	}

	pubInfo := peer.AddrInfo{
		ID: publisherID,
	}
	// Sync all the links in the hamt, since so far we have only synced the root.
	for _, e := range hn.Hamt.Data {
		if e.HashMapNode != nil {
			nodeCid := (*e.HashMapNode).(cidlink.Link).Cid
			// Gather all the HAMT Cids so that we can remove them from
			// datastore once finished processing.
			err = ing.sub.SyncHAMTEntries(ctx, pubInfo, nodeCid, dagsync.ScopedBlockHook(gatherCids))
			if err != nil {
				wrappedErr := fmt.Errorf("failed to sync remaining HAMT: %w", err)
				if errors.Is(err, ipld.ErrNotExists{}) || strings.Contains(err.Error(), "content not found") {
					return 0, adIngestError{adIngestContentNotFound, wrappedErr}
				}
				return 0, adIngestError{adIngestSyncEntriesErr, wrappedErr}
			}
		}
	}

	var mhCount int

	// Start processing now that the entire HAMT is synced. HAMT is a map,
	// and we are using the keys in the map to represent multihashes.
	// Therefore, we only care about the keys.
	//
	// Group the mutlihashes in StoreBatchSize batches and process as usual.
	mhs := make([]multihash.Multihash, 0, batchSize)
	mi := hn.MapIterator()
	for !mi.Done() {
		k, _, err := mi.Next()
		if err != nil {
			return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("faild to iterate through HAMT: %w", err)}
		}
		ks, err := k.AsString()
		if err != nil {
			return mhCount, adIngestError{adIngestMalformedErr, fmt.Errorf("HAMT key must be of type string: %w", err)}
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
		if len(mhs) >= batchSize {
			if err = ing.indexAdMultihashes(ad, providerID, mhs, log); err != nil {
				return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
			}
			mhCount += len(mhs)
			mhs = mhs[:0]
		}
	}
	// Process any remaining multihashes from the batch cut-off.
	if len(mhs) > 0 {
		if err = ing.indexAdMultihashes(ad, providerID, mhs, log); err != nil {
			return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
		}
		mhCount += len(mhs)
	}

	return mhCount, nil
}

func (ing *Ingester) ingestEntriesFromPublisher(ctx context.Context, ad schema.Advertisement, publisherID, providerID peer.ID, entsCid cid.Cid, log *zap.SugaredLogger) (int, error) {
	log = log.With("entriesKind", "EntryChunk")

	// We have already peeked at the first EntryChunk as part of probing
	// the entries type, so process that first.
	chunk, err := ing.loadEntryChunk(entsCid)
	if err != nil {
		// Node was loaded previously, so must be a permanent data error.
		return 0, adIngestError{adIngestEntryChunkErr, fmt.Errorf("failed to load first entry chunk: %w", err)}
	}

	err = ing.ingestEntryChunk(ctx, ad, providerID, entsCid, *chunk, log)
	if err != nil {
		// There was an error storing the multihashes.
		return 0, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest first entry chunk: %w", err)}
	}

	mhCount := len(chunk.Entries)

	// Sync all remaining entry chunks.
	if chunk.Next != nil {
		blockHook := func(p peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
			// Load CID as entry chunk.
			chunk, err := ing.loadEntryChunk(c)
			if err != nil {
				actions.FailSync(adIngestError{adIngestIndexerErr, fmt.Errorf("failed to load entry chunk: %w", err)})
				return
			}
			err = ing.ingestEntryChunk(ctx, ad, providerID, c, *chunk, log)
			if err != nil {
				actions.FailSync(adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest entry chunk: %w", err)})
				return
			}
			mhCount += len(chunk.Entries)

			if chunk.Next == nil {
				actions.SetNextSyncCid(cid.Undef)
				return
			}
			actions.SetNextSyncCid(chunk.Next.(cidlink.Link).Cid)
		}

		pubInfo := peer.AddrInfo{
			ID: publisherID,
		}
		// Traverse remaining entry chunks until end of chain or recursion
		// depth reached.
		err = ing.sub.SyncEntries(ctx, pubInfo, chunk.Next.(cidlink.Link).Cid, dagsync.ScopedBlockHook(blockHook))
		if err != nil {
			var adIngestErr adIngestError
			if errors.As(err, &adIngestErr) {
				return mhCount, err
			}
			wrappedErr := fmt.Errorf("failed to sync entries: %w", err)
			if errors.Is(err, ipld.ErrNotExists{}) || strings.Contains(err.Error(), "content not found") {
				return mhCount, adIngestError{adIngestContentNotFound, wrappedErr}
			}
			return mhCount, adIngestError{adIngestSyncEntriesErr, wrappedErr}
		}
	}

	return mhCount, nil
}

func (ing *Ingester) ingestEntriesFromCar(ctx context.Context, ad schema.Advertisement, providerID peer.ID, adCid, entsCid cid.Cid, log *zap.SugaredLogger) (int, error) {
	// Create a context to cancel reading entries.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adBlock, err := ing.mirror.read(ctx, adCid, false)
	if err != nil {
		return 0, err
	}

	var firstEntryBlock carstore.EntryBlock
	var ok bool
	if adBlock.Entries != nil {
		firstEntryBlock, ok = <-adBlock.Entries
	}
	if !ok {
		// This advertisement has no entries because they were removed later in
		// the chain and the indexer ingesting this advertisement does not know
		// that yet, or the publisher was not serving content for the
		// advertisement's entries CID when this CAR file was created.
		return 0, adIngestError{adIngestContentNotFound, errors.New("advertisement has no entries")}
	}
	if firstEntryBlock.Cid != entsCid {
		return 0, fmt.Errorf("advertisement entries cid does not match first entry chunk cid in car file")
	}

	chunk, err := firstEntryBlock.EntryChunk()
	if err != nil {
		return 0, err
	}

	log = log.With("entriesKind", "CarEntryChunk")

	err = ing.ingestEntryChunk(ctx, ad, providerID, entsCid, *chunk, log)
	if err != nil {
		return 0, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest entry chunk: %w", err)}
	}
	mhCount := len(chunk.Entries)

	for entryBlock := range adBlock.Entries {
		if entryBlock.Err != nil {
			return mhCount, err
		}
		chunk, err = entryBlock.EntryChunk()
		if err != nil {
			return mhCount, fmt.Errorf("failed to decode entry chunk from car file data: %w", err)
		}
		err = ing.ingestEntryChunk(ctx, ad, providerID, entryBlock.Cid, *chunk, log)
		if err != nil {
			return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest entry chunk: %w", err)}
		}
		mhCount += len(chunk.Entries)
	}

	return mhCount, nil
}

// ingestEntryChunk ingests a block of entries as that block is received
// through graphsync.
//
// When each advertisement on a chain is processed by ingestAd, that
// advertisement's entries are synced in a separate dagsync.Subscriber.Sync
// operation. This function is used as a scoped block hook, and is called for
// each block that is received.
func (ing *Ingester) ingestEntryChunk(ctx context.Context, ad schema.Advertisement, providerID peer.ID, entryChunkCid cid.Cid, chunk schema.EntryChunk, log *zap.SugaredLogger) error {
	err := ing.indexAdMultihashes(ad, providerID, chunk.Entries, log)
	if !ing.mirror.canWrite() {
		// Done processing entries chunk, so remove from datastore.
		if err := ing.dsTmp.Delete(ctx, datastore.NewKey(entryChunkCid.String())); err != nil {
			log.Errorw("Error deleting index from datastore", "err", err)
		}
	}
	if err != nil {
		return fmt.Errorf("failed processing entries for advertisement: %w", err)
	}
	return nil
}

// indexAdMultihashes filters out invalid multihashes and indexes those
// remaining in the indexer core.
func (ing *Ingester) indexAdMultihashes(ad schema.Advertisement, providerID peer.ID, mhs []multihash.Multihash, log *zap.SugaredLogger) error {
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
		panic("removing individual multihashes not allowed")
	}

	if err := ing.indexer.Put(value, mhs...); err != nil {
		return fmt.Errorf("cannot put multihashes into indexer: %w", err)
	}
	log.Infow("Indexed multihashes from chunk", "count", len(mhs), "sample", mhs[0].B58String())

	return nil
}

func (ing *Ingester) loadAd(c cid.Cid) (schema.Advertisement, error) {
	adn, err := ing.loadNode(c, schema.AdvertisementPrototype)
	if err != nil {
		return schema.Advertisement{}, err
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
		return nil, err
	}
	return schema.UnwrapEntryChunk(node)
}

func (ing *Ingester) loadHamt(c cid.Cid) (*hamt.Node, error) {
	node, err := ing.loadNode(c, hamt.HashMapRootPrototype)
	if err != nil {
		return nil, err
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
	val, err := ing.dsTmp.Get(context.Background(), datastore.NewKey(c.String()))
	if err != nil {
		return nil, fmt.Errorf("cannot fetch the node from datastore: %w", err)
	}
	node, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), prototype)
	if err != nil {
		return nil, fmt.Errorf("failed to decode ipldNode: %w", err)
	}
	return node, nil

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

// isAdvertisement checks if an IPLD node is an advertisement, by looking to
// see if it has a "Signature" field. Additional checks may be needed if the
// schema is extended with new types that are traversable.
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}

// isHAMT checks if the given IPLD node is a HAMT root node by looking for a
// field named "hamt".
//
// See: https://github.com/ipld/go-ipld-adl-hamt/blob/master/schema.ipldsch
func isHAMT(n ipld.Node) bool {
	h, _ := n.LookupByString("hamt")
	return h != nil
}

func stringsToMultiaddrs(addrs []string) []multiaddr.Multiaddr {
	maddrs, err := mautil.StringsToMultiaddrs(addrs)
	if err != nil {
		log.Warnw("Bad address in advertisement", "err", err)
	}
	return maddrs
}
