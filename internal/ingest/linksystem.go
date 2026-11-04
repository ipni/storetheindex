package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"math/bits"
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
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/mautil"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	mc "github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/twmb/murmur3"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"

	// Import so these codecs get registered.
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

// maxDigestLength is the longest multihash digest that ingest indexes, as the
// IPNI specification sets it. A multihash with a longer digest is ignored and
// the rest of the ad is indexed.
const maxDigestLength = 128

var (
	// errBadAdvert is returned by the link system for an advertisement that
	// does not decode as one, fails validation, or names a provider that is
	// not a valid peer ID.
	errBadAdvert = fmt.Errorf("%w: bad advertisement", ipnisync.ErrMalformed)
	// errInvalidAdvertSignature is returned by the link system for an
	// advertisement whose signature, or the signature of one of its extended
	// provider records, does not verify.
	errInvalidAdvertSignature = fmt.Errorf("%w: invalid advertisement signature", ipnisync.ErrMalformed)
	// errPublishNotAllowed is returned for an advertisement signed by a
	// publisher that the policy does not allow to publish for its provider.
	// On the advertisement chain the policy can be reloaded, so a later sync
	// may accept the ad. An entries block that fails this way can never be an
	// entry chunk or a HAMT node, so entriesSyncError treats it as malformed.
	errPublishNotAllowed = errors.New("advertisement signer not allowed to publish for provider")
	// errMalformedHamt is wrapped by the errors of a HAMT whose structure does
	// not match the HAMT specification.
	errMalformedHamt = fmt.Errorf("%w: malformed HAMT", ipnisync.ErrMalformed)
	// errBadIPLDData is returned by the link system for a block that does not
	// decode in the codec of its CID.
	errBadIPLDData = fmt.Errorf("%w: bad ipld data", ipnisync.ErrMalformed)
	// errInternal is wrapped by errors from the indexer itself, such as a
	// failure of its datastore or value store. Such an error may not happen
	// again, so the ad is retried. Its details are not shown to users: the
	// provider's last error holds only this error's text.
	errInternal = errors.New("internal error")
	// errNotInDatastore is returned by loadNode for a block that is not in
	// the temporary datastore. The block may have been deleted by another
	// worker that synced the same block, so a later sync may find it.
	errNotInDatastore = errors.New("node not present on indexer")
)

// maxHamtMapLen is the largest HAMT bitmap, in bytes, that ingest accepts. It
// is the bitmap of a HAMT with a bit width of 16.
const maxHamtMapLen = 1 << 13

// syncStatusCtxKey is the context key used to carry a byte counter into the
// link system's write opener, so that downloaded entry-block bytes can be
// attributed to a publisher's sync status.
type syncStatusCtxKey struct{}

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
				return errBadIPLDData
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
			// Attribute downloaded bytes to the publisher's sync status, if a
			// counter is present in the context (set during entry sync).
			if bc, ok := lctx.Ctx.Value(syncStatusCtxKey{}).(interface{ AddBytes(int64) }); ok && bc != nil {
				bc.AddBytes(int64(len(origBuf)))
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
		return "", errPublishNotAllowed
	}

	return provID, nil
}

// adExtendedProviders returns the extended providers of ad for the registry,
// or nil when the ad has none. It ignores what the IPNI specification says to
// ignore: the extended providers of a removal ad, those with Override set on
// an ad without a context ID, and a provider record with no valid address. A
// record with an undecodable peer ID is ignored too. The record of the ad's
// own provider may omit its addresses, which then come from the ad.
func adExtendedProviders(ad schema.Advertisement, log *zap.SugaredLogger) *registry.ExtendedProviders {
	switch {
	case ad.ExtendedProvider == nil:
		return nil
	case ad.IsRm:
		log.Warn("Ignoring extended providers on a removal advertisement")
		return nil
	case len(ad.ContextID) == 0 && ad.ExtendedProvider.Override:
		log.Warn("Ignoring extended providers with override set on an advertisement without context id")
		return nil
	}

	log.Debug("Advertisement has extended providers")
	extendedProviders := &registry.ExtendedProviders{
		ContextualProviders: make(map[string]registry.ContextualExtendedProviders),
	}

	// Creating ExtendedProviderInfo record for each of the providers.
	// Keeping in mind that the provider from the outer ad doesn't need to be included into the extended providers list
	eProvs := make([]registry.ExtendedProviderInfo, 0, len(ad.ExtendedProvider.Providers))
	for _, ep := range ad.ExtendedProvider.Providers {
		epID, err := peer.Decode(ep.ID)
		if err != nil {
			log.Warnw("Ignoring extended provider with invalid peer id", "err", err)
			continue
		}
		// The IPNI specification says to ignore a provider record with no
		// addresses. The record of the ad's own provider may omit them,
		// and then has the ad's addresses.
		addrs := stringsToMultiaddrs(ep.Addresses)
		if len(addrs) == 0 && ep.ID == ad.Provider {
			addrs = stringsToMultiaddrs(ad.Addresses)
		}
		if len(addrs) == 0 {
			log.Warnw("Ignoring extended provider with no valid address", "extendedProvider", epID)
			continue
		}

		eProvs = append(eProvs, registry.ExtendedProviderInfo{
			PeerID:   epID,
			Metadata: ep.Metadata,
			Addrs:    addrs,
		})
	}

	// An ad whose records are all ignored has extended providers as if it had
	// none, so the registry keeps those it has. An empty list of records
	// replaces them.
	if len(eProvs) == 0 && len(ad.ExtendedProvider.Providers) != 0 {
		log.Warn("Ignoring extended providers, since every record is ignored")
		return nil
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
	return extendedProviders
}

// ingestAd fetches all the entries for a single advertisement and processes
// them. This is called for each advertisement in a synced chain by an ingester
// worker. The worker begins processing the synced advertisement chain when it
// receives notification that a peer has finished a sync for advertisements.
//
// Advertisements are processed from oldest to newest, which is the reverse
// order that they were received in.
//
// The publisherID is the peer ID of the message publisher. This is not
// necessarily the same as the provider ID in the advertisement. The publisher
// is the source of the indexed content, the provider is where content can be
// retrieved from. It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) ingestAd(
	ctx context.Context,
	publisherID peer.ID,
	adCid cid.Cid,
	resync, frozen bool,
	lag int,
	headProvider peer.AddrInfo,
	wkrNum int,
) (
	hasEnts bool,
	source adDataSource,
	location string,
	err error,
) {
	log := log.With("publisher", publisherID, "adCid", adCid, "worker", wkrNum)

	ad, err := ing.loadAd(adCid)
	if err != nil {
		stats.Record(context.Background(), metrics.AdLoadError.M(1))
		log.Errorw("Failed to load advertisement, skipping", "err", err)
		// The ad cannot be loaded, so we cannot process it. Return nil so that
		// the ad is marked as processed and is removed from the datastore.
		return false, adDataSourceNone, "", nil
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

		if mhCount != 0 {
			// Record multihashes rate per provider.
			elapsed = now.Sub(entsSyncStart)
			ing.ingestRates.Update(string(headProvider.ID), uint64(mhCount), elapsed)

			// Record how long entries sync took.
			elapsedMsec = float64(elapsed.Nanoseconds()) / 1e6
			stats.Record(ctx, metrics.EntriesSyncLatency.M(elapsedMsec))
		}
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

	extendedProviders := adExtendedProviders(ad, log)

	// If head ad does not have provider addresses, then update using addresses
	// from the current advertisement.
	if len(headProvider.Addrs) == 0 {
		headProvider.Addrs = stringsToMultiaddrs(ad.Addresses)
	}

	// Register provider or update existing registration. The provider must be
	// allowed by policy to be registered.
	log.Debug("Updating provider registry with latest ad info")
	err = ing.reg.Update(ctx, headProvider, publisher, adCid, extendedProviders, lag)
	if err != nil {
		// A registry.ErrMissingProviderAddr error is not considered a
		// permanent adIngestMalformedErr error, because an advertisement added
		// to the chain in the future may have a valid address than can be
		// used, allowing all the previous ads without valid addresses to be
		// processed.
		return false, adDataSourceNone, "", adIngestError{adIngestRegisterProviderErr, fmt.Errorf("could not update provider info: %w", err)}
	}

	log = log.With("contextID", base64.StdEncoding.EncodeToString(ad.ContextID))

	if ad.IsRm {
		log.Info("Advertisement is for removal by context id")
		err = ing.indexer.RemoveProviderContext(providerID, ad.ContextID)
		if err != nil {
			return false, adDataSourceNone, "", adIngestError{adIngestIndexerErr, fmt.Errorf("%w: failed to remove provider context: %w", errInternal, err)}
		}
		return false, adDataSourceNone, "", nil
	}

	if len(ad.Metadata) == 0 {
		// If the ad has no metadata and no entries, then the ad is only for
		// updating provider addresses. Otherwise it is an error.
		if ad.Entries != schema.NoEntries {
			return false, adDataSourceNone, "", adIngestError{adIngestMalformedErr, fmt.Errorf("advertisement missing metadata")}
		}
		log.Info("Advertisement is for provider address update only")
		return false, adDataSourceNone, "", nil
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
			log.Info("Indexer frozen, advertisement only updates metadata")
		} else {
			log.Info("Advertisement is for metadata update only")
		}
		err = ing.indexer.Put(value)
		if err != nil {
			return false, adDataSourceNone, "", adIngestError{adIngestIndexerErr, fmt.Errorf("%w: failed to update metadata: %w", errInternal, err)}
		}
		return false, adDataSourceNone, "", nil
	}

	entriesCid := ad.Entries.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		return false, adDataSourceNone, "", adIngestError{adIngestMalformedErr, errors.New("advertisement entries link is undefined")}
	}
	// dagsync applies the identity size limit when syncing from the publisher.
	// Entries read from a CAR file are checked here and in
	// ingestCarEntryStream.
	if err = ipnisync.CheckIdentitySize(entriesCid); err != nil {
		return false, adDataSourceNone, "", adIngestError{adIngestMalformedErr, fmt.Errorf("advertisement entries link: %w", err)}
	}

	log.Debug("Advertisement has entries to sync")

	// Mark the downloading phase and attach a byte counter so the link system
	// write opener can attribute downloaded entry-block bytes to this sync.
	syncStatus := ing.SyncStatusFor(publisherID)
	syncStatus.SetDownloading()
	ctx = context.WithValue(ctx, syncStatusCtxKey{}, syncStatus)

	if ing.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer cancel()
	}

	entsSyncStart = time.Now()

	// If using a CAR reader, then try to get the advertisement CAR file first.
	// Main is preferred (especially on resync). A CAR is used only when its
	// entries chain is complete; otherwise ingest tries External, then the
	// publisher, and rewrites a writable Main from that source.
	if ing.mirror.canRead() {
		log.Debug("Attempting to fetch entries from CAR mirror")
		carReadStart := time.Now()
		mhCount, adSource, adSourceLocation, err := ing.ingestEntriesFromCar(ctx, ad, publisherID, providerID, adCid, entriesCid, log)
		hasEnts := mhCount != 0
		// If entries data successfully read from a complete CAR file.
		if err == nil {
			elapsedMsec := float64(time.Since(carReadStart).Nanoseconds()) / 1e6
			stats.RecordWithOptions(ctx,
				stats.WithMeasurements(metrics.CarMirrorReadLatency.M(elapsedMsec)),
				stats.WithTags(
					tag.Insert(metrics.AdSource, adSource.String()),
					tag.Insert(metrics.Location, adSourceLocation),
				))
			ing.mhsFromMirror.Add(uint64(mhCount))
			return hasEnts, adSource, adSourceLocation, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			log.Warnw("Cannot get advertisement from CAR mirror", "err", err)
			var adIngestErr adIngestError
			if errors.As(err, &adIngestErr) {
				switch adIngestErr.state {
				case adIngestIndexerErr:
					// Could not store multihashes in core, so stop trying to index ad.
					return hasEnts, adDataSourceNone, "", err
				case adIngestMalformedErr:
					// A Next link in the CAR's entries chain is an identity
					// CID over the size limit. The Entries link itself is
					// checked before the mirror is read, so it never gets
					// here. A sync from the publisher refuses the same link,
					// so there is no fallback.
					return hasEnts, adDataSourceNone, "", err
				case adIngestContentNotFound:
					// No entries data in CAR file. Entries data deleted later
					// in chain unknown to this indexer, or publisher not
					// serving entries data.
					return hasEnts, adDataSourceNone, "", err
				}
			} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return hasEnts, adDataSourceNone, "", err
			}
			// Incomplete, empty, or otherwise unusable CAR: fetch from publisher.
		} else {
			log.Debug("Advertisement not found in CAR mirror")
		}
	}
	log.Debug("Fetching entries from publisher")
	providerFetchStart := time.Now()

	// The ad.Entries link can point to either a chain of EntryChunks or a
	// HAMT. Sync the very first entry so that we can check which type it is.
	// This means the maximum depth of entries traversal will be 1 plus the
	// configured max depth.
	err = ing.sub.SyncOneEntry(ctx, publisher, entriesCid)
	if err != nil {
		return false, adDataSourceNone, "", entriesSyncError(err, "failed to sync first entry while checking entries type")
	}

	node, err := ing.loadNode(entriesCid, basicnode.Prototype.Any)
	if err != nil {
		return false, adDataSourceNone, "", adIngestError{adIngestIndexerErr, fmt.Errorf("failed to load first entry after sync: %w", err)}
	}

	if isHAMT(node) {
		log.Info("syncing hamt entries")
		mhCount, err = ing.ingestHamtFromPublisher(ctx, ad, publisherID, providerID, entriesCid, log)
	} else {
		log.Info("syncing entries")
		mhCount, err = ing.ingestEntriesFromPublisher(ctx, ad, publisherID, providerID, entriesCid, log)
	}

	// Record how long it took to fetch all the entries from the publisher.
	if err == nil {
		elapsedMsec := float64(time.Since(providerFetchStart).Nanoseconds()) / 1e6
		stats.Record(ctx, metrics.ProviderFetchLatency.M(elapsedMsec))
	}

	return mhCount != 0, adDataSourceProvider, "", err
}

// entriesSyncError classifies an error from syncing entries from the
// publisher. An error that wraps ipnisync.ErrMalformed comes from content that
// no later sync can change, so the ad is skipped and the rest of the
// publisher's chain is still ingested. dagsync returns such an error for a
// block over its size limit or an identity CID over the digest limit, and the
// link system for a block that does not decode or that it takes for an
// advertisement that is not valid. errPublishNotAllowed is malformed too: a
// block the link system takes for an advertisement cannot be an entry chunk or
// a HAMT node, whether or not the policy later allows its signer. Missing
// content is not found. After any other error a later sync may succeed.
func entriesSyncError(err error, msg string) adIngestError {
	wrappedErr := fmt.Errorf("%s: %w", msg, err)
	switch {
	case errors.Is(err, ipnisync.ErrMalformed),
		errors.Is(err, errPublishNotAllowed):
		return adIngestError{adIngestMalformedErr, wrappedErr}
	case errors.Is(err, ipld.ErrNotExists{}) || strings.Contains(err.Error(), "content not found"):
		return adIngestError{adIngestContentNotFound, wrappedErr}
	default:
		return adIngestError{adIngestSyncEntriesErr, wrappedErr}
	}
}

// entryChunkLoadError classifies an error from loading an entry chunk that a
// sync stored in dsTmp. The link system stored the block only after it
// decoded, so loading it fails because reading dsTmp failed or the block is
// gone from it, which a retry may fix, or because the block is not an
// EntryChunk, which no retry fixes.
func entryChunkLoadError(err error, msg string) adIngestError {
	wrappedErr := fmt.Errorf("%s: %w", msg, err)
	switch {
	case errors.Is(err, errInternal), errors.Is(err, errNotInDatastore):
		return adIngestError{adIngestIndexerErr, wrappedErr}
	default:
		return adIngestError{adIngestEntryChunkErr, wrappedErr}
	}
}

func (ing *Ingester) ingestHamtFromPublisher(ctx context.Context, ad schema.Advertisement, publisherID, providerID peer.ID, entsCid cid.Cid, log *zap.SugaredLogger) (int, error) {
	// Split HAMP into batches of 4096 entries.
	const batchSize = 4096

	log = log.With("entriesKind", "hamt")
	syncStatus := ing.SyncStatusFor(publisherID)
	// Keep track of all CIDs in the HAMT to remove them later when the
	// processing is done.
	hamtCids := []cid.Cid{entsCid}
	gatherCids := func(_ peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		hamtCids = append(hamtCids, c)
	}
	// HAMT entries do not get written to mirror, so delete them.
	defer func() {
		for _, c := range hamtCids {
			err := ing.dsTmp.Delete(ctx, datastore.NewKey(c.String()))
			if err != nil {
				log.Errorw("Error deleting HAMT cid from datastore", "cid", c, "err", err)
			}
		}
	}()

	// Load the CID as HAMT root node.
	root, err := ing.loadHamtRoot(entsCid)
	if err != nil {
		return 0, hamtLoadError(err, "failed to load entries as HAMT root node")
	}

	pubInfo := peer.AddrInfo{
		ID: publisherID,
	}
	// Sync the nodes below the root, since so far only the root is synced.
	// hamtKeys checks the HAMT as it reads it.
	for _, elem := range root.Hamt.Data {
		if elem.HashMapNode == nil {
			continue
		}
		lnk, ok := (*elem.HashMapNode).(cidlink.Link)
		if !ok {
			continue
		}
		// Gather all the HAMT Cids so that we can remove them from datastore
		// once finished processing.
		err = ing.sub.SyncHAMTEntries(ctx, pubInfo, lnk.Cid, dagsync.ScopedBlockHook(gatherCids))
		if err != nil {
			return 0, entriesSyncError(err, "failed to sync remaining HAMT")
		}
	}

	var mhCount int

	// HAMT is a map, and we are using the keys in the map to represent
	// multihashes. Therefore, we only care about the keys.
	//
	// The multihashes are indexed in batches, so that only one batch of
	// multihashes from the HAMT is held in memory. hamtKeys checks each node
	// as it reaches it. When a check fails, the batches already indexed stay
	// indexed, as the chunks before a bad entry chunk do, and the multihashes
	// of the batch in progress are not indexed.
	mhs := make([]multihash.Multihash, 0, batchSize)
	for key, err := range ing.hamtKeys(ctx, root) {
		if err != nil {
			return mhCount, hamtLoadError(err, "invalid HAMT")
		}
		mhs = append(mhs, multihash.Multihash(key))
		if len(mhs) < batchSize {
			continue
		}
		if err = ing.indexAdMultihashes(ad, providerID, mhs, log); err != nil {
			return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
		}
		mhCount += len(mhs)
		syncStatus.AddHamtMultihashes(len(mhs))
		mhs = mhs[:0]
	}
	// Process any remaining multihashes from the batch cut-off.
	if len(mhs) > 0 {
		if err = ing.indexAdMultihashes(ad, providerID, mhs, log); err != nil {
			return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
		}
		mhCount += len(mhs)
		syncStatus.AddHamtMultihashes(len(mhs))
	}

	return mhCount, nil
}

func (ing *Ingester) ingestEntriesFromPublisher(ctx context.Context, ad schema.Advertisement, publisherID, providerID peer.ID, entsCid cid.Cid, log *zap.SugaredLogger) (int, error) {
	log = log.With("entriesKind", "EntryChunk")
	syncStatus := ing.SyncStatusFor(publisherID)

	// We have already peeked at the first EntryChunk as part of probing
	// the entries type, so process that first.
	chunk, err := ing.loadEntryChunk(entsCid)
	if err != nil {
		loadErr := entryChunkLoadError(err, "failed to load first entry chunk")
		if loadErr.state == adIngestEntryChunkErr {
			ing.deleteUnmirroredBlock(ctx, entsCid, log)
		}
		return 0, loadErr
	}

	err = ing.indexAdMultihashes(ad, providerID, chunk.Entries, log)
	if err != nil {
		// There was an error storing the multihashes.
		return 0, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest first entry chunk: %w", err)}
	}
	mhCount := len(chunk.Entries)
	syncStatus.AddChunk(len(chunk.Entries))

	// Done with the chunk. A writable mirror keeps it until the CAR is written.
	ing.deleteUnmirroredBlock(ctx, entsCid, log)

	// Sync all remaining entry chunks.
	if chunk.Next != nil {
		blockHook := func(p peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
			// Load CID as entry chunk.
			chunk, err := ing.loadEntryChunk(c)
			if err != nil {
				loadErr := entryChunkLoadError(err, "failed to load entry chunk")
				if loadErr.state == adIngestEntryChunkErr {
					ing.deleteUnmirroredBlock(ctx, c, log)
				}
				actions.FailSync(loadErr)
				return
			}
			err = ing.indexAdMultihashes(ad, providerID, chunk.Entries, log)
			if err != nil {
				actions.FailSync(adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest entry chunk: %w", err)})
				return
			}
			mhCount += len(chunk.Entries)
			syncStatus.AddChunk(len(chunk.Entries))

			// Done with the chunk. A writable mirror keeps it until the CAR is written.
			ing.deleteUnmirroredBlock(ctx, c, log)

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
			return mhCount, entriesSyncError(err, "failed to sync entries")
		}
	}

	return mhCount, nil
}

// deleteUnmirroredBlock removes a block from the temporary datastore when
// the mirror will not write it into a CAR. A writable mirror keeps the block
// until the advertisement is written or cleaned up.
func (ing *Ingester) deleteUnmirroredBlock(ctx context.Context, c cid.Cid, log *zap.SugaredLogger) {
	if ing.mirror.canWrite() {
		return
	}
	if err := ing.dsTmp.Delete(ctx, datastore.NewKey(c.String())); err != nil {
		log.Errorw("Error deleting entries block from datastore", "cid", c, "err", err)
	}
}

func (ing *Ingester) ingestEntriesFromCar(
	ctx context.Context,
	ad schema.Advertisement,
	publisherID, providerID peer.ID,
	adCid, entsCid cid.Cid,
	log *zap.SugaredLogger,
) (
	mhCount int,
	source adDataSource,
	adSourcelocation string,
	err error,
) {
	ingestOpened := func(adBlock *carstore.AdBlock, src adDataSource, loc string) (int, adDataSource, string, error) {
		defer adBlock.Close()
		n, ingestErr := ing.ingestCarEntryStream(ctx, ad, publisherID, providerID, entsCid, adBlock, src, log)
		if ingestErr != nil {
			recordCarUnusable(src, loc, ingestErr)
			return n, adDataSourceNone, "", ingestErr
		}
		return n, src, loc, nil
	}

	adBlock, mainLocation, readErr := ing.mirror.readMain(ctx, adCid, false)
	switch {
	case readErr == nil:
		n, src, loc, ingestErr := ingestOpened(adBlock, adDataSourceMain, mainLocation)
		if ingestErr == nil {
			return n, src, loc, nil
		}
		if _, ok := errors.AsType[carstore.ErrUnusable](ingestErr); !ok {
			// Some critical error, not a CAR file issue.
			return n, src, loc, ingestErr
		}
		log.Infow("Main CAR unusable, trying another source", "err", ingestErr)

	case errors.Is(readErr, fs.ErrNotExist):
		// Miss; try External.

	case errors.Is(readErr, context.Canceled), errors.Is(readErr, context.DeadlineExceeded):
		return 0, adDataSourceNone, "", readErr

	default:
		recordCarUnusable(adDataSourceMain, ing.mirror.mainLocation(), readErr)
		log.Infow("Cannot read Main CAR, trying another source", "err", readErr)
	}

	adBlock, adSource, adSourcelocation, err := ing.mirror.readExternalRace(ctx, adCid, false)
	if err != nil {
		return 0, adDataSourceNone, "", err
	}

	return ingestOpened(adBlock, adSource, adSourcelocation)
}

func (ing *Ingester) ingestCarEntryStream(
	ctx context.Context,
	ad schema.Advertisement,
	publisherID, providerID peer.ID,
	entsCid cid.Cid,
	adBlock *carstore.AdBlock,
	adSource adDataSource,
	log *zap.SugaredLogger,
) (mhCount int, err error) {
	if adBlock.Entries == nil {
		return 0, fmt.Errorf("%w: advertisement has no entries", carstore.ErrUnusableNoEntries)
	}

	log = log.With("entriesKind", "CarEntryChunk")
	syncStatus := ing.SyncStatusFor(publisherID)
	copyMirrorData := ing.mirror.canWrite() && adSource.canBeWritten()

	expectedCid := entsCid
	for entryBlock := range adBlock.Entries {
		if entryBlock.Err != nil {
			return mhCount, entryBlock.Err
		}
		if expectedCid == cid.Undef {
			return mhCount, fmt.Errorf("%w: extra entry block %s beyond the entries chain", carstore.ErrUnusableExtraEntries, entryBlock.Cid)
		}
		if entryBlock.Cid != expectedCid {
			return mhCount, fmt.Errorf("%w: entry block %s is not the next expected chunk %s", carstore.ErrUnusableUnexpectedEntry, entryBlock.Cid, expectedCid)
		}

		chunk, err := entryBlock.EntryChunk()
		if err != nil {
			if errors.Is(err, carstore.ErrHAMT) {
				return mhCount, err
			}
			return mhCount, fmt.Errorf("%w: cannot decode entry chunk %s: %v", carstore.ErrUnusableDecodeEntry, entryBlock.Cid, err)
		}

		// Index now: this chunk is the next expected CID, so it is authentic
		// even if the rest of the CAR is later unusable. Put is idempotent, so
		// fallback to External or the publisher can re-index the same mappings.
		err = ing.indexAdMultihashes(ad, providerID, chunk.Entries, log)
		if err != nil {
			return mhCount, adIngestError{adIngestIndexerErr, fmt.Errorf("failed to ingest entry chunk: %w", err)}
		}
		mhCount += len(chunk.Entries)
		syncStatus.AddChunk(len(chunk.Entries))

		if copyMirrorData {
			if err = ing.dsTmp.Put(ctx, datastore.NewKey(entryBlock.Cid.String()), entryBlock.Data); err != nil {
				return mhCount, fmt.Errorf("cannot write entry chunk data from mirror to datastore: %w", err)
			}
		}

		if chunk.Next == nil {
			expectedCid = cid.Undef
		} else {
			expectedCid = chunk.Next.(cidlink.Link).Cid
			if err = ipnisync.CheckIdentitySize(expectedCid); err != nil {
				return mhCount, adIngestError{adIngestMalformedErr, fmt.Errorf("entries next link: %w", err)}
			}
		}
	}
	if expectedCid != cid.Undef {
		return mhCount, fmt.Errorf("%w: missing remaining entries starting at %s", carstore.ErrUnusableIncompleteEntries, expectedCid)
	}

	return mhCount, nil
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
		} else if len(decoded.Digest) > maxDigestLength {
			if badMultihashCount == 0 {
				log.Warnw("Multihash digest too long, ignoring", "digestSize", len(decoded.Digest), "maxDigestSize", maxDigestLength)
			}
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
		return fmt.Errorf("%w: cannot put multihashes into indexer: %w", errInternal, err)
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

func (ing *Ingester) loadEntryChunk(c cid.Cid) (schema.EntryChunk, error) {
	node, err := ing.loadNode(c, schema.EntryChunkPrototype)
	if err != nil {
		return schema.EntryChunk{}, err
	}
	entChunk, err := schema.UnwrapEntryChunk(node)
	if err != nil {
		return schema.EntryChunk{}, fmt.Errorf("cannot decode entry chunk: %w", err)
	}
	return *entChunk, nil
}

func (ing *Ingester) loadHamtRoot(c cid.Cid) (*hamt.HashMapRoot, error) {
	node, err := ing.loadNode(c, hamt.HashMapRootPrototype)
	if err != nil {
		return nil, err
	}
	root, ok := bindnode.Unwrap(node).(*hamt.HashMapRoot)
	if !ok || root == nil {
		return nil, fmt.Errorf("%w: cannot unwrap node as HAMT root", errMalformedHamt)
	}
	return root, nil
}

func (ing *Ingester) loadHamtNode(c cid.Cid) (*hamt.HashMapNode, error) {
	node, err := ing.loadNode(c, hamt.HashMapNodePrototype.Representation())
	if err != nil {
		return nil, err
	}
	hn, ok := bindnode.Unwrap(node).(*hamt.HashMapNode)
	if !ok || hn == nil {
		return nil, fmt.Errorf("%w: cannot unwrap node as HAMT node", errMalformedHamt)
	}
	return hn, nil
}

// hamtLoadError classifies an error from reading a HAMT held in the temporary
// datastore. A datastore failure, or a node gone from the datastore because
// another worker that synced the same node deleted it, is an indexer error,
// and the ad can be retried. Any other error comes from the HAMT data and is
// permanent.
func hamtLoadError(err error, msg string) error {
	wrappedErr := fmt.Errorf("%s: %w", msg, err)
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, errInternal), errors.Is(err, errNotInDatastore):
		return adIngestError{adIngestIndexerErr, wrappedErr}
	default:
		return adIngestError{adIngestMalformedErr, wrappedErr}
	}
}

// hamtKeys returns the keys in the buckets of the HAMT at root, in the order
// of the HAMT, loading child nodes from the temporary datastore.
//
// It checks the HAMT against the HAMT specification as it reads it: the
// root's hash algorithm is identity, sha2-256 or murmur3; its bucket size is
// at least 1; the bitmap of every node has the root's length, which is a power
// of two of at most maxHamtMapLen bytes; the bitmap has one bit set for each
// element; each element is either a link or a bucket of 1 to BucketSize
// entries; and the hash of each key selects the slots on the path from the
// root to the key's bucket, so a node can be reached by one path only. At the
// first check that fails, it yields the error and stops.
func (ing *Ingester) hamtKeys(ctx context.Context, root *hamt.HashMapRoot) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		mapLen := len(root.Hamt.Map)
		switch {
		case root.HashAlg != mc.Identity && root.HashAlg != mc.Sha2_256 && root.HashAlg != mc.Murmur3X64_64:
			yield(nil, fmt.Errorf("%w: unsupported hash algorithm %s", errMalformedHamt, root.HashAlg))
			return
		case mapLen == 0 || mapLen&(mapLen-1) != 0 || mapLen > maxHamtMapLen:
			yield(nil, fmt.Errorf("%w: bitmap length %d is not a power of two up to %d", errMalformedHamt, mapLen, maxHamtMapLen))
			return
		case root.BucketSize < 1:
			yield(nil, fmt.Errorf("%w: bucket size %d is less than 1", errMalformedHamt, root.BucketSize))
			return
		}
		bitWidth := bits.TrailingZeros(uint(mapLen)) + 3

		var nodeKeysIter func(node *hamt.HashMapNode, path []int) (keepGoing bool, err error)
		nodeKeysIter = func(node *hamt.HashMapNode, path []int) (keepGoing bool, err error) {
			if len(node.Map) != mapLen {
				return false, fmt.Errorf("%w: node bitmap length %d differs from root bitmap length %d", errMalformedHamt, len(node.Map), mapLen)
			}
			// A node below the root holds at least one element, so every
			// subtree holds a key, and a node reached by a second path fails
			// the check of the first key below it.
			if len(path) != 0 && len(node.Data) == 0 {
				return false, fmt.Errorf("%w: node below the root has no elements", errMalformedHamt)
			}
			// Each bit set in the bitmap takes the next element of data, in
			// order. The first bit is the most significant bit of the first
			// byte.
			data := node.Data
			for slot := range 8 * mapLen {
				if node.Map[slot>>3]&(0x80>>(slot&7)) == 0 {
					continue
				}

				if len(data) == 0 {
					return false, fmt.Errorf("%w: bitmap has more bits set than the node has elements", errMalformedHamt)
				}
				elem := data[0]
				data = data[1:]
				subPath := append(path, slot)

				if (elem.Bucket == nil) == (elem.HashMapNode == nil) {
					return false, fmt.Errorf("%w: element is not exactly one of link or bucket", errMalformedHamt)
				}
				if elem.Bucket != nil {
					if len(*elem.Bucket) == 0 || len(*elem.Bucket) > root.BucketSize {
						return false, fmt.Errorf("%w: bucket has %d entries, want 1 to %d", errMalformedHamt, len(*elem.Bucket), root.BucketSize)
					}
					for _, entry := range *elem.Bucket {
						if err := checkHamtKey(root.HashAlg, entry.Key, subPath, bitWidth); err != nil {
							return false, err
						}
						if !yield(entry.Key, nil) {
							return false, nil
						}
					}
					continue
				}

				if err := ctx.Err(); err != nil {
					return false, err
				}
				lnk, ok := (*elem.HashMapNode).(cidlink.Link)
				if !ok {
					return false, fmt.Errorf("%w: link is not a CID", errMalformedHamt)
				}
				child, err := ing.loadHamtNode(lnk.Cid)
				if err != nil {
					return false, fmt.Errorf("cannot load HAMT node %s: %w", lnk.Cid, err)
				}
				if ok, err := nodeKeysIter(child, subPath); !ok {
					return false, err
				}
			}
			if len(data) != 0 {
				return false, fmt.Errorf("%w: node has more elements than bits set in its bitmap", errMalformedHamt)
			}
			return true, nil
		}
		path := make([]int, 0, 256/bitWidth)
		if _, err := nodeKeysIter(&root.Hamt, path); err != nil {
			yield(nil, err)
		}
	}
}

// checkHamtKey checks that the hash of key selects the slots of path, as the
// HAMT specification places a key: at depth d, the slot is the d-th group of
// bitWidth bits of the hash, from its most significant bit.
func checkHamtKey(hashAlg mc.Code, key []byte, path []int, bitWidth int) error {
	var hash []byte
	switch hashAlg {
	case mc.Identity:
		hash = key
	case mc.Sha2_256:
		sum := sha256.Sum256(key)
		hash = sum[:]
	case mc.Murmur3X64_64:
		h := murmur3.New128()
		h.Write(key)
		hash = h.Sum(nil)
	}
	if len(path)*bitWidth > len(hash)*8 {
		return fmt.Errorf("%w: key is at depth %d, deeper than its hash allows", errMalformedHamt, len(path))
	}
	for depth, slot := range path {
		if hashBits(hash, depth*bitWidth, bitWidth) != slot {
			return fmt.Errorf("%w: key is not in the slot that its hash selects", errMalformedHamt)
		}
	}
	return nil
}

// hashBits returns n bits of hash starting at bit from, counted from the
// most significant bit of the first byte.
func hashBits(hash []byte, from, n int) int {
	var v int
	for i := from; i < from+n; i++ {
		v = v<<1 | int(hash[i/8]>>(7-i%8)&1)
	}
	return v
}

func (ing *Ingester) loadNode(c cid.Cid, prototype ipld.NodePrototype) (ipld.Node, error) {
	val, err := ing.dsTmp.Get(context.Background(), datastore.NewKey(c.String()))
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, errNotInDatastore
		}
		return nil, fmt.Errorf("%w: cannot fetch the node from datastore: %w", errInternal, err)
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
