package reaper

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	leveldb "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	// Import so these codecs get registered.
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

var log = logging.Logger("ipni-gc")

var ErrProviderNotFound = errors.New("provider not found")

var errIndexerWrite = errors.New("delete from index valuestore failed")

type GCStats struct {
	AdsProcessed    int
	CarsDataSize    int64
	CarsRemoved     int
	CtxIDsKept      int
	CtxIDsRemoved   int
	IndexAdsKept    int
	IndexAdsRemoved int
	IndexesRemoved  int
	RemovalAds      int
	ReusedCtxIDs    int
}

type Reaper struct {
	carDelete bool
	carReader *carstore.CarReader
	commit    bool
	dstore    datastore.Batching
	dstoreTmp datastore.Batching
	fileStore filestore.Interface
	host      host.Host
	hostOwner bool
	indexer   indexer.Interface
	pcache    *pcache.ProviderCache
	stats     GCStats
	sub       *dagsync.Subscriber
	tmpDir    string
}

type GCAd struct {
	Previous  cid.Cid
	ContextID string
	Entries   cid.Cid
}

type GCState struct {
	LastProcessedAdCid cid.Cid
}

func dsContextPrefix(publisherID peer.ID, contextID string) string {
	return fmt.Sprintf("/gc/%s/ctx/%s/", publisherID.String(), contextID)
}

func dsGCStateKey(publisherID peer.ID) string {
	return fmt.Sprintf("/gc/%s/state", publisherID.String())
}

func New(idxr indexer.Interface, fileStore filestore.Interface, options ...Option) (*Reaper, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	var hostOwner bool
	if opts.p2pHost == nil {
		opts.p2pHost, err = libp2p.New()
		if err != nil {
			return nil, err
		}
		hostOwner = true
	}

	var carReader *carstore.CarReader
	if fileStore != nil && opts.carRead {
		carReader, err = carstore.NewReader(fileStore, carstore.WithCompress(opts.carCompAlg))
		if err != nil {
			return nil, fmt.Errorf("cannot create car file reader: %w", err)
		}
	}

	// Create datastore
	dstore, err := createDatastore(opts.dstoreDir)
	if err != nil {
		return nil, err
	}

	// Create datastore for temporary ad data.
	if opts.dstoreTmpDir == "" {
		opts.dstoreTmpDir, err = os.MkdirTemp("", "ipni-gc-tmpstore")
		if err != nil {
			return nil, fmt.Errorf("cannot create temp directory for gc: %w", err)
		}
	}
	dstoreTmp, err := createDatastore(opts.dstoreTmpDir)
	if err != nil {
		return nil, err
	}

	sub, err := makeSubscriber(opts.p2pHost, dstoreTmp, opts.topic, opts.httpTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to start dagsync subscriber: %w", err)
	}

	if opts.pcache == nil {
		opts.pcache, err = pcache.New(pcache.WithRefreshInterval(0),
			pcache.WithSourceURL("localhost:3000"))
		if err != nil {
			return nil, err
		}
	}

	return &Reaper{
		carDelete: opts.carDelete,
		carReader: carReader,
		commit:    opts.commit,
		dstore:    dstore,
		dstoreTmp: dstoreTmp,
		fileStore: fileStore,
		host:      opts.p2pHost,
		hostOwner: hostOwner,
		indexer:   idxr,
		pcache:    opts.pcache,
		sub:       sub,
		tmpDir:    opts.dstoreTmpDir,
	}, nil
}

func (r *Reaper) Close() {
	if r.sub != nil {
		if err := r.sub.Close(); err != nil {
			log.Errorw("Failed to close ipni dagsync subscriber", "err", err)
		}

	}
	if r.hostOwner {
		if err := r.host.Close(); err != nil {
			log.Errorw("Failed to close libp2p host", "err", err)
		}
	}
	if r.dstore != nil {
		r.dstore.Close()
	}
	if r.dstoreTmp != nil {
		r.dstoreTmp.Close()
	}
	if r.tmpDir != "" {
		os.RemoveAll(r.tmpDir)
	}
}

func (r *Reaper) ClearStats() {
	r.stats = GCStats{}
}

func (r *Reaper) Stats() GCStats {
	return r.stats
}

func (r *Reaper) Reap(ctx context.Context, providerID peer.ID) error {
	pinfo, err := r.pcache.Get(ctx, providerID)
	if err != nil {
		return err
	}
	if pinfo == nil {
		return ErrProviderNotFound
	}

	if pinfo.Publisher == nil {
		return errors.New("provider has not publisher")
	}

	latestAdCid := pinfo.LastAdvertisement
	publisher := *pinfo.Publisher

	log := log.With("provider", providerID)
	if providerID != publisher.ID {
		log = log.With("publisher", publisher.ID)
	}

	log.Infow("Doing GC for provider", "latestAd", latestAdCid)
	if latestAdCid == cid.Undef {
		log.Info("No advertisements ingested to GC")
		return nil
	}

	gcState, err := r.loadProviderGCState(ctx, providerID)
	if err != nil {
		return fmt.Errorf("failed to load gc state for provider: %w", err)
	}

	if gcState.LastProcessedAdCid == latestAdCid {
		log.Info("No new advertisements for GC to process. GC has already processed the latest.")
		return nil
	}

	// Sync unprocessed portion of chain from publisher.
	_, err = r.sub.SyncAdChain(ctx, publisher,
		dagsync.WithHeadAdCid(latestAdCid),
		dagsync.WithStopAdCid(gcState.LastProcessedAdCid),
	)
	if err != nil {
		return fmt.Errorf("failed to sync advertisement chain: %w", err)
	}
	log.Debug("Synced unprocessed portion of advertisement chain from publisher")

	removedCtxSet := make(map[string]struct{})
	remaining := make(map[string][]cid.Cid)

	for adCid := latestAdCid; adCid != gcState.LastProcessedAdCid; {
		ad, err := r.loadAd(adCid)
		if err != nil {
			return fmt.Errorf("gc failed to load advertisement %s: %w", adCid.String(), err)
		}
		contextID := base64.StdEncoding.EncodeToString(ad.ContextID)
		if ad.IsRm {
			log.Debugw("GC processing removal ad", "adCid", adCid)
			r.stats.RemovalAds++
			reused, ok := remaining[contextID]
			if ok {
				r.stats.ReusedCtxIDs++
				log.Warnw("ContextID reused after removal", "removedIn", adCid, "reusedIn", reused)
			} else {
				if _, ok = removedCtxSet[contextID]; !ok {
					removedCtxSet[contextID] = struct{}{}
					err = r.removePrevRemaining(ctx, providerID, publisher, contextID)
					if err != nil {
						return fmt.Errorf("failed to remove ads with deleted context: %w", err)
					}
				}
			}
			// Do not keep removal car files.
			if err = r.removeCarFile(ctx, adCid); err != nil {
				return err
			}
		} else if ad.Entries == nil || ad.Entries == schema.NoEntries {
			log.Debugw("GC processing no-content ad", "adCid", adCid)
			// Remove CAR file of empty ad.
			if err = r.removeCarFile(ctx, adCid); err != nil {
				return err
			}
		} else {
			_, ok := removedCtxSet[contextID]
			if ok {
				log.Debugw("GC processing removed index content ad", "adCid", adCid)
				r.stats.IndexAdsRemoved++
				// Ad's context ID is removed, so delete all multihashes for ad.
				if err = r.deleteEntries(ctx, adCid, providerID, publisher); err != nil {
					return fmt.Errorf("could not delete advertisement content: %w", err)
				}
				// Do not keep removed CAR file.
				if err = r.removeCarFile(ctx, adCid); err != nil {
					return err
				}
			} else {
				log.Debugw("GC processing index content ad", "adCid", adCid)
				r.stats.IndexAdsKept++
				// Append ad cid to list of remaining for this context ID.
				remaining[contextID] = append(remaining[contextID], adCid)
			}
		}
		r.stats.AdsProcessed++

		if ad.PreviousID == nil {
			adCid = cid.Undef
		} else {
			adCid = ad.PreviousID.(cidlink.Link).Cid
		}
	}
	log.Debugw("Done processing advertisements", "count", r.stats.AdsProcessed)

	// Record which ads remain undeleted.
	if err = r.saveRemaining(ctx, publisher.ID, remaining); err != nil {
		return err
	}
	r.stats.CtxIDsKept += len(remaining)
	r.stats.CtxIDsRemoved += len(removedCtxSet)

	// Update GC state.
	gcState.LastProcessedAdCid = latestAdCid
	return r.saveProviderGCState(ctx, publisher.ID, gcState)
}

func (r *Reaper) removePrevRemaining(ctx context.Context, providerID peer.ID, publisher peer.AddrInfo, contextID string) error {
	prefix := dsContextPrefix(publisher.ID, contextID)
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	}
	results, err := r.dstore.Query(ctx, q)
	if err != nil {
		return err
	}

	ents, err := results.Rest()
	if err != nil {
		return err
	}

	log.Debugw("Deleting indexes from previously processed ads that have removed contextID", "count", len(ents))
	for i := range ents {
		key := ents[i].Key
		adCid, err := cid.Decode(path.Base(key))
		if err != nil {
			log.Errorw("Cannot decode remaining advertisement cid", "err", err)
			if r.commit {
				if err = r.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
					return err
				}
			}
			continue
		}
		if err = r.deleteEntries(ctx, adCid, providerID, publisher); err != nil {
			return fmt.Errorf("could not delete advertisement content: %w", err)
		}
		if r.commit {
			if err = r.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
				return err
			}
		}
		r.stats.IndexAdsRemoved++
	}

	if !r.commit {
		return nil
	}
	return r.dstore.Sync(ctx, datastore.NewKey(prefix))
}

func (r *Reaper) saveRemaining(ctx context.Context, pubID peer.ID, remaining map[string][]cid.Cid) error {
	if !r.commit {
		return nil
	}
	for contextID, adCids := range remaining {
		ctxPrefix := dsContextPrefix(pubID, contextID)
		for _, adCid := range adCids {
			remainingKey := datastore.NewKey(ctxPrefix + adCid.String())
			err := r.dstore.Put(ctx, remainingKey, []byte{})
			if err != nil {
				return fmt.Errorf("failed to write remaining ads to datastore: %w", err)
			}
		}
	}
	return nil
}

func (r *Reaper) removeCarFile(ctx context.Context, adCid cid.Cid) error {
	if r.carReader == nil || !r.carDelete {
		return nil
	}
	carPath := r.carReader.CarPath(adCid)
	file, err := r.fileStore.Head(ctx, carPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	log.Debugw("Deleting CAR file", "path", carPath, "size", file.Size)
	if r.commit {
		if err = r.fileStore.Delete(ctx, carPath); err != nil {
			return fmt.Errorf("failed to remove CAR file: %w", err)
		}
	}
	r.stats.CarsRemoved++
	r.stats.CarsDataSize += file.Size
	return nil
}

func makeSubscriber(host host.Host, dstoreTmp datastore.Batching, topic string, httpTimeout time.Duration) (*dagsync.Subscriber, error) {
	linksys := cidlink.DefaultLinkSystem()

	linksys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := dstoreTmp.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	linksys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return dstoreTmp.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}

	return dagsync.NewSubscriber(host, dstoreTmp, linksys, topic, dagsync.HttpTimeout(httpTimeout))
}

func (r *Reaper) loadProviderGCState(ctx context.Context, providerID peer.ID) (GCState, error) {
	if r.dstore == nil {
		return GCState{}, nil
	}

	gcStateData, err := r.dstore.Get(ctx, datastore.NewKey(dsGCStateKey(providerID)))
	if err != nil {
		if err != datastore.ErrNotFound {
			return GCState{}, fmt.Errorf("could not read gc state from datastore: %w", err)
		}
		return GCState{}, nil
	}
	var gcState GCState
	err = json.Unmarshal(gcStateData, &gcState)
	if err != nil {
		return GCState{}, fmt.Errorf("cannot decode gc state data: %w", err)
	}

	return gcState, nil
}

func (r *Reaper) saveProviderGCState(ctx context.Context, providerID peer.ID, gcState GCState) error {
	if r.dstore == nil || !r.commit {
		return nil
	}

	data, err := json.Marshal(&gcState)
	if err != nil {
		return fmt.Errorf("cannot encode gc state data: %w", err)
	}

	key := datastore.NewKey(dsGCStateKey(providerID))
	err = r.dstore.Put(ctx, key, data)
	if err != nil {
		return err
	}

	return r.dstore.Sync(ctx, key)
}

func (r *Reaper) deleteEntries(ctx context.Context, adCid cid.Cid, providerID peer.ID, publisher peer.AddrInfo) error {
	var mhCount int
	var err error
	var source string
	if r.carReader != nil {
		mhCount, err = r.removeEntriesWithCar(ctx, adCid, providerID)
		if err != nil {
			if errors.Is(err, errIndexerWrite) {
				return err
			}
			log.Errorw("Cannot get advertisement from car store", "err", err)
		}
		source = "CAR file"
	}
	if r.carReader == nil || err != nil {
		mhCount, err = r.removeEntriesWithPublisher(ctx, adCid, providerID, publisher)
		if err != nil {
			if errors.Is(err, errIndexerWrite) {
				return err
			}
			log.Errorw("Cannot get advertisement from publisher", "err", err)
			return errors.New("cannot get index data from advertisement to delete")
		}
		source = "synced from publisher"
	}
	log.Infow("Deleted indexes for removed contextID", "count", mhCount, "provider", providerID, "source", source)
	r.stats.IndexesRemoved += mhCount
	return nil
}

func (r *Reaper) removeEntriesWithCar(ctx context.Context, adCid cid.Cid, providerID peer.ID) (int, error) {
	// Create a context to cancel reading entries.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adBlock, err := r.carReader.Read(ctx, adCid, false)
	if err != nil {
		return 0, err
	}

	if adBlock.Entries == nil {
		// This advertisement has no entries because they were removed later in
		// the chain and were not saved, or the publisher was not serving
		// content for the advertisement's entries CID when this CAR file was
		// created.
		return 0, nil
	}

	ad, err := adBlock.Advertisement()
	if err != nil {
		return 0, fmt.Errorf("failed to decode advertisement from car file: %w", err)
	}
	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	var mhCount int
	for entryBlock := range adBlock.Entries {
		if entryBlock.Err != nil {
			return mhCount, entryBlock.Err
		}
		chunk, err := entryBlock.EntryChunk()
		if err != nil {
			return mhCount, fmt.Errorf("failed to decode entry chunk from car file data: %w", err)
		}
		if len(chunk.Entries) == 0 {
			continue
		}
		if r.commit {
			if err = r.indexer.Remove(value, chunk.Entries...); err != nil {
				return mhCount, fmt.Errorf("%w: %w", errIndexerWrite, err)
			}
		}
		mhCount += len(chunk.Entries)
	}

	return mhCount, nil
}

func (r *Reaper) removeEntriesWithPublisher(ctx context.Context, adCid cid.Cid, providerID peer.ID, publisher peer.AddrInfo) (int, error) {
	/*
		_, err := sub.SyncAdChain(ctx, publisher, dagsync.WithHeadAdCid(adCid), dagsync.ScopedDepthLimit(int64(1)))
		if err != nil {
			return err
		}
		err := sub.SyncEntries(ctx, publisher, entsCid)
		if err != nil {
			return err
		}
	*/
	return 0, errors.New("not implemented")
}

func createDatastore(dir string) (datastore.Batching, error) {
	if err := fsutil.DirWritable(dir); err != nil {
		return nil, err
	}
	ds, err := leveldb.NewDatastore(dir, nil)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func (r *Reaper) loadAd(c cid.Cid) (schema.Advertisement, error) {
	adn, err := r.loadNode(c, schema.AdvertisementPrototype)
	if err != nil {
		return schema.Advertisement{}, err
	}
	ad, err := schema.UnwrapAdvertisement(adn)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	return *ad, nil
}

func (r *Reaper) loadNode(c cid.Cid, prototype ipld.NodePrototype) (ipld.Node, error) {
	val, err := r.dstoreTmp.Get(context.Background(), datastore.NewKey(c.String()))
	if err != nil {
		return nil, err
	}
	return decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), prototype)
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(codec uint64, r io.Reader, prototype ipld.NodePrototype) (ipld.Node, error) {
	nb := prototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		return nil, err
	}
	if err = decoder(nb, r); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
