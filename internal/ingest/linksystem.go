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

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/libp2p/go-libp2p-core/peer"
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
func (ing *Ingester) ingestAd(publisherID peer.ID, adCid cid.Cid, ad schema.Advertisement) error {
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

	var maddrs []multiaddr.Multiaddr
	if len(ad.Addresses) != 0 {
		maddrs = make([]multiaddr.Multiaddr, 0, len(ad.Addresses))
		for _, addr := range ad.Addresses {
			maddr, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				log.Warnw("Bad address in advertisement", "address", addr)
				continue
			}
			maddrs = append(maddrs, maddr)
		}
	}

	provider := peer.AddrInfo{
		ID:    providerID,
		Addrs: maddrs,
	}
	err = ing.reg.RegisterOrUpdate(context.Background(), provider, adCid, publisher)
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

	// If advertisement has no entries, then it is for updating metadata only.
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

	ctx := context.Background()
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
		if strings.Contains(err.Error(), "content not found") {
			return adIngestError{adIngestContentNotFound, wrappedErr}
		}
		return adIngestError{adIngestSyncEntriesErr, wrappedErr}
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
		gatherCids := func(_ peer.ID, c cid.Cid, _ legs.SegmentSyncActions) {
			hamtCids = append(hamtCids, c)
		}
		defer func() {
			for _, c := range hamtCids {
				err := ing.ds.Delete(ctx, datastore.NewKey(c.String()))
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
					legs.ScopedBlockHook(gatherCids),
					// Disable segmented sync.
					//
					// TODO: see if segmented sync for HAMT makes sense and if
					// so modify block hook action above appropriately.
					legs.ScopedSegmentDepthLimit(-1))
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
				err := ing.indexAdMultihashes(ad, mhs, log)
				if err != nil {
					return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
				}
				mhCount += len(mhs)
				mhs = mhs[:0]
			}
		}
		// Process any remaining multihashes from the batch cut-off.
		if len(mhs) > 0 {
			err := ing.indexAdMultihashes(ad, mhs, log)
			if err != nil {
				return adIngestError{adIngestIndexerErr, fmt.Errorf("failed to index content from HAMT: %w", err)}
			}
			mhCount += len(mhs)
		}
		entsStoreElapsed = time.Since(entsStoreStart)
	} else {
		log = log.With("entriesKind", "EntryChunk")

		var chunkFuncs chan func()
		var asyncDone chan struct{}
		var asyncEntries bool

		if !ing.writeEntriesSynchronously() {
			asyncEntries = true
			asyncDone = make(chan struct{})
			chunkFuncs = make(chan func(), 1)
			go func(fch <-chan func()) {
				for f := range fch {
					f()
				}
				close(asyncDone)
			}(chunkFuncs)
		}

		// We have already peeked at the first EntryChunk as part of probing
		// the entries type, so process that first.
		chunk, err := ing.loadEntryChunk(syncedFirstEntryCid)
		if err != nil {
			errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
		} else {
			chunkStart := time.Now()
			if asyncEntries {
				chunkFuncs <- func() {
					if err := ing.ingestEntryChunk(ctx, ad, syncedFirstEntryCid, *chunk, log); err != nil {
						errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
					}
				}
			} else {
				err = ing.ingestEntryChunk(ctx, ad, syncedFirstEntryCid, *chunk, log)
				if err != nil {
					errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
				}
			}
			mhCount += len(chunk.Entries)
			entsStoreElapsed += time.Since(chunkStart)
		}

		if chunk != nil && chunk.Next != nil {
			var errCh chan error
			if asyncEntries {
				errCh = make(chan error, 1)
			}
			nextChunkCid := chunk.Next.(cidlink.Link).Cid
			// Traverse remaining entry chunks based on the entries selector
			// that limits recursion depth.
			_, err = ing.sub.Sync(ctx, publisherID, nextChunkCid, ing.entriesSel, nil, legs.ScopedBlockHook(func(p peer.ID, c cid.Cid, actions legs.SegmentSyncActions) {
				chunkStart := time.Now()

				// Load CID as entry chunk since the selector should only
				// select entry chunk nodes.
				chunk, err := ing.loadEntryChunk(c)
				if err != nil {
					actions.FailSync(err)
					errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
					return
				}
				if asyncEntries {
					// If an error occurred, cause the segment sync to fail
					// with that error.
					select {
					default:
					case err = <-errCh:
						actions.FailSync(err)
						return
					}
					chnk := *chunk
					chunkFuncs <- func() {
						if err := ing.ingestEntryChunk(ctx, ad, c, chnk, log); err != nil {
							errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
							// Tell segment sync to fail with the err.
							select {
							case errCh <- err:
							default:
							}
						}
					}
				} else {
					err = ing.ingestEntryChunk(ctx, ad, c, *chunk, log)
					if err != nil {
						actions.FailSync(err)
						errsIngestingEntryChunks = append(errsIngestingEntryChunks, err)
						return
					}
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
				if asyncEntries {
					close(chunkFuncs)
					<-asyncDone
				}
				wrappedErr := fmt.Errorf("failed to sync entries: %w", err)
				if strings.Contains(err.Error(), "content not found") {
					return adIngestError{adIngestContentNotFound, wrappedErr}
				}
				return adIngestError{adIngestSyncEntriesErr, wrappedErr}
			}
		}
		if asyncEntries {
			close(chunkFuncs)
			<-asyncDone
		}
	}
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
// advertisement's entries are synced in a separate legs.Subscriber.Sync
// operation. This function is used as a scoped block hook, and is called for
// each block that is received.
func (ing *Ingester) ingestEntryChunk(ctx context.Context, ad schema.Advertisement, entryChunkCid cid.Cid, chunk schema.EntryChunk, log *zap.SugaredLogger) error {
	defer func() {
		// Remove the content block from the data store now that processing it
		// has finished. This prevents storing redundant information in several
		// datastores.
		entryChunkKey := datastore.NewKey(entryChunkCid.String())
		err := ing.ds.Delete(ctx, entryChunkKey)
		if err != nil {
			log.Errorw("Error deleting index from datastore", "err", err)
		}
	}()

	err := ing.indexAdMultihashes(ad, chunk.Entries, log)
	if err != nil {
		return fmt.Errorf("failed processing entries for advertisement: %w", err)
	}

	ing.signalMetricsUpdate()
	return nil
}

// indexAdMultihashes indexes the content multihashes in a block of data. First
// the advertisement is loaded to get the context ID and metadata. Then the
// metadata and multihashes in the content block are indexed by the
// indexer-core.
func (ing *Ingester) indexAdMultihashes(ad schema.Advertisement, mhs []multihash.Multihash, log *zap.SugaredLogger) error {
	// Load the advertisement data for this chunk. If there are more chunks to
	// follow, then cache the ad data.
	value, isRm, err := getAdData(ad)
	if err != nil {
		return err
	}

	// Iterate over all entries and ingest remove bad ones.
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

	if isRm {
		if err := ing.indexer.Remove(value, mhs...); err != nil {
			return fmt.Errorf("cannot remove multihashes from indexer: %w", err)
		}
		log.Infow("Removed multihashes in entry chunk", "count", len(mhs))
	} else {
		if err := ing.indexer.Put(value, mhs...); err != nil {
			return fmt.Errorf("cannot put multihashes into indexer: %w", err)
		}
		log.Infow("Put multihashes in entry chunk", "count", len(mhs))
	}
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
	val, err := ing.ds.Get(context.Background(), key)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch the node from datastore: %w", err)
	}
	return decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), prototype)
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
