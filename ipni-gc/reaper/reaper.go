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
	"path/filepath"
	"sync"
	"time"

	"github.com/gammazero/targz"
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

const gcDataArchivePrefix = "ipnigc-"

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
	TimeElapsed     time.Duration
}

type Reaper struct {
	carDelete   bool
	carReader   *carstore.CarReader
	commit      bool
	dsDir       string
	dsTmpDir    string
	fileStore   filestore.Interface
	host        host.Host
	hostOwner   bool
	httpTimeout time.Duration
	indexer     indexer.Interface
	pcache      *pcache.ProviderCache
	stats       GCStats
	statsMutex  sync.Mutex
	topic       string
}

type scythe struct {
	reaper *Reaper

	dstore     datastore.Batching
	dstoreTmp  datastore.Batching
	providerID peer.ID
	publisher  peer.AddrInfo
	sub        *dagsync.Subscriber
	stats      GCStats
	tmpDir     string
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

const (
	dsGCStateKey = "/state"
)

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

	if opts.pcache == nil {
		opts.pcache, err = pcache.New(pcache.WithPreload(false),
			pcache.WithRefreshInterval(0),
			pcache.WithSourceURL("http://localhost:3000"))
		if err != nil {
			return nil, err
		}
	}

	return &Reaper{
		carDelete:   opts.carDelete,
		carReader:   carReader,
		commit:      opts.commit,
		dsDir:       opts.dstoreDir,
		dsTmpDir:    opts.dstoreTmpDir,
		fileStore:   fileStore,
		host:        opts.p2pHost,
		hostOwner:   hostOwner,
		httpTimeout: opts.httpTimeout,
		indexer:     idxr,
		pcache:      opts.pcache,
		topic:       opts.topic,
	}, nil
}

func (r *Reaper) Close() {
	if r.hostOwner {
		if err := r.host.Close(); err != nil {
			log.Errorw("Failed to close libp2p host", "err", err)
		}
	}
}

func (r *Reaper) AddStats(a GCStats) {
	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()

	r.stats.AdsProcessed += a.AdsProcessed
	r.stats.CarsDataSize += a.CarsDataSize
	r.stats.CarsRemoved += a.CarsRemoved
	r.stats.CtxIDsKept += a.CtxIDsKept
	r.stats.CtxIDsRemoved += a.CtxIDsRemoved
	r.stats.IndexAdsKept += a.IndexAdsKept
	r.stats.IndexAdsRemoved += a.IndexAdsRemoved
	r.stats.IndexesRemoved += a.IndexesRemoved
	r.stats.RemovalAds += a.RemovalAds
	r.stats.ReusedCtxIDs += a.ReusedCtxIDs
	r.stats.TimeElapsed += a.TimeElapsed
}

func (r *Reaper) ClearStats() {
	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()
	r.stats = GCStats{}
}

func (r *Reaper) Stats() GCStats {
	r.statsMutex.Lock()
	defer r.statsMutex.Unlock()
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
	publisher := *pinfo.Publisher

	// If the gc state datastore does not already exist, try to get archive
	// from filestore.
	dstoreDir := filepath.Join(r.dsDir, dstoreDirName(publisher.ID))
	_, err = os.Stat(dstoreDir)
	if errors.Is(err, fs.ErrNotExist) {
		if err = unarchiveDatastore(ctx, publisher.ID, r.fileStore, r.dsDir); err != nil {
			return fmt.Errorf("failed to retireve datastore from archive: %w", err)
		}
	}
	// Check that the extracted archive directory is readable, and if it does
	// not exist create a new gc datastore directory.
	dstore, err := createDatastore(dstoreDir)
	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}
	defer func() {
		if dstore != nil {
			dstore.Close()
		}
	}()

	// Create datastore for temporary ad data.
	var tmpDir string
	if r.dsTmpDir == "" {
		tmpDir, err = os.MkdirTemp("", "gc-tmp-"+publisher.ID.String())
		if err != nil {
			return fmt.Errorf("cannot create temp directory for gc: %w", err)
		}
		defer os.RemoveAll(tmpDir)
	} else {
		if err = fsutil.DirWritable(r.dsTmpDir); err != nil {
			return err
		}
		tmpDir = filepath.Join(r.dsTmpDir, "gc-tmp-"+publisher.ID.String())
		defer func() {
			if tmpDir != "" {
				os.RemoveAll(tmpDir)
			}
		}()
	}
	dstoreTmp, err := createDatastore(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create temporary datastore: %w", err)
	}
	defer dstoreTmp.Close()

	// Create ipni dagsync Subscriber for this publisher.
	sub, err := makeSubscriber(r.host, dstoreTmp, r.topic, r.httpTimeout)
	if err != nil {
		return fmt.Errorf("failed to start dagsync subscriber: %w", err)
	}
	defer sub.Close()

	s := &scythe{
		reaper:     r,
		dstore:     dstore,
		dstoreTmp:  dstoreTmp,
		providerID: providerID,
		publisher:  publisher,
		sub:        sub,
		tmpDir:     tmpDir,
	}

	startTime := time.Now()

	err = s.reap(ctx, pinfo.LastAdvertisement)
	if err != nil {
		tmpDir = ""
		return err
	}

	s.stats.TimeElapsed = time.Since(startTime)

	log.Infow("GC stats for provider", "provider", providerID,
		"AdsProcessed:", s.stats.AdsProcessed,
		"CarsDataSize:", s.stats.CarsDataSize,
		"CarsRemoved:", s.stats.CarsRemoved,
		"CtxIDsKept:", s.stats.CtxIDsKept,
		"CtxIDsRemoved:", s.stats.CtxIDsRemoved,
		"IndexAdsKept:", s.stats.IndexAdsKept,
		"IndexAdsRemoved:", s.stats.IndexAdsRemoved,
		"IndexesRemoved:", s.stats.IndexesRemoved,
		"RemovalAds:", s.stats.RemovalAds,
		"ReusedCtxIDs:", s.stats.ReusedCtxIDs,
		"TimeElapsed:", s.stats.TimeElapsed,
	)

	r.AddStats(s.stats)

	dstore.Close()
	dstore = nil

	return s.archiveDatastore(ctx, dstoreDir)
}

func (r *Reaper) DataArchiveName(ctx context.Context, providerID peer.ID) (string, error) {
	pinfo, err := r.pcache.Get(ctx, providerID)
	if err != nil {
		return "", err
	}
	if pinfo == nil {
		return "", ErrProviderNotFound
	}

	if pinfo.Publisher == nil {
		return "", errors.New("provider has not publisher")
	}
	return dstoreArchiveName(pinfo.Publisher.ID), nil
}

func (s *scythe) reap(ctx context.Context, latestAdCid cid.Cid) error {
	log.Infow("Starting GC for provider", "latestAd", latestAdCid, "provider", s.providerID)
	if latestAdCid == cid.Undef {
		log.Info("No advertisements ingested to GC")
		return nil
	}

	gcState, err := s.loadGCState(ctx)
	if err != nil {
		return fmt.Errorf("failed to load gc state for provider: %w", err)
	}

	if gcState.LastProcessedAdCid == latestAdCid {
		log.Info("No new advertisements for GC to process. GC has already processed the latest.")
		return nil
	}

	// Sync unprocessed portion of chain from publisher.
	_, err = s.sub.SyncAdChain(ctx, s.publisher,
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
		ad, err := s.loadAd(adCid)
		if err != nil {
			return fmt.Errorf("gc failed to load advertisement %s: %w", adCid.String(), err)
		}
		contextID := base64.StdEncoding.EncodeToString(ad.ContextID)
		if ad.IsRm {
			log.Debugw("GC processing removal ad", "adCid", adCid)
			s.stats.RemovalAds++
			reused, ok := remaining[contextID]
			if ok {
				s.stats.ReusedCtxIDs++
				log.Warnw("ContextID reused after removal", "removedIn", adCid, "reusedIn", reused)
			} else {
				if _, ok = removedCtxSet[contextID]; !ok {
					removedCtxSet[contextID] = struct{}{}
					err = s.reapPrevRemaining(ctx, contextID)
					if err != nil {
						return fmt.Errorf("failed to remove ads with deleted context: %w", err)
					}
				}
			}
			// Do not keep removal car files.
			if err = s.removeCarFile(ctx, adCid); err != nil {
				return err
			}
		} else if ad.Entries == nil || ad.Entries == schema.NoEntries {
			log.Debugw("GC processing no-content ad", "adCid", adCid)
			// Remove CAR file of empty ad.
			if err = s.removeCarFile(ctx, adCid); err != nil {
				return err
			}
		} else {
			_, ok := removedCtxSet[contextID]
			if ok {
				log.Debugw("GC removed index content ad", "adCid", adCid)
				s.stats.IndexAdsRemoved++
				// Ad's context ID is removed, so delete all multihashes for ad.
				if err = s.deleteEntries(ctx, adCid); err != nil {
					return fmt.Errorf("could not delete advertisement content: %w", err)
				}
				// Do not keep removed CAR file.
				if err = s.removeCarFile(ctx, adCid); err != nil {
					return err
				}
			} else {
				log.Debugw("GC processing index content ad", "adCid", adCid)
				s.stats.IndexAdsKept++
				// Append ad cid to list of remaining for this context ID.
				remaining[contextID] = append(remaining[contextID], adCid)
			}
		}
		s.stats.AdsProcessed++

		if ad.PreviousID == nil {
			adCid = cid.Undef
		} else {
			adCid = ad.PreviousID.(cidlink.Link).Cid
		}
	}
	log.Debugw("Done processing advertisements", "count", s.stats.AdsProcessed)

	// Record which ads remain undeleted.
	if err = s.saveRemaining(ctx, remaining); err != nil {
		return err
	}
	s.stats.CtxIDsKept += len(remaining)
	s.stats.CtxIDsRemoved += len(removedCtxSet)

	// Update GC state.
	gcState.LastProcessedAdCid = latestAdCid
	return s.saveGCState(ctx, gcState)
}

func (s *scythe) reapPrevRemaining(ctx context.Context, contextID string) error {
	prefix := dsContextPrefix(s.publisher.ID, contextID)
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	}
	results, err := s.dstore.Query(ctx, q)
	if err != nil {
		return err
	}

	ents, err := results.Rest()
	if err != nil {
		return err
	}

	commit := s.reaper.commit

	log.Debugw("Deleting indexes from previously processed ads that have removed contextID", "count", len(ents))
	for i := range ents {
		key := ents[i].Key
		adCid, err := cid.Decode(path.Base(key))
		if err != nil {
			log.Errorw("Cannot decode remaining advertisement cid", "err", err)
			if commit {
				if err = s.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
					return err
				}
			}
			continue
		}
		if err = s.deleteEntries(ctx, adCid); err != nil {
			return fmt.Errorf("could not delete advertisement content: %w", err)
		}
		if commit {
			if err = s.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
				return err
			}
		}
		s.stats.IndexAdsRemoved++
	}

	if !commit {
		return nil
	}
	return s.dstore.Sync(ctx, datastore.NewKey(prefix))
}

func (s *scythe) saveRemaining(ctx context.Context, remaining map[string][]cid.Cid) error {
	if !s.reaper.commit {
		return nil
	}
	pubID := s.publisher.ID
	for contextID, adCids := range remaining {
		ctxPrefix := dsContextPrefix(pubID, contextID)
		for _, adCid := range adCids {
			remainingKey := datastore.NewKey(ctxPrefix + adCid.String())
			err := s.dstore.Put(ctx, remainingKey, []byte{})
			if err != nil {
				return fmt.Errorf("failed to write remaining ads to datastore: %w", err)
			}
		}
	}
	return nil
}

func (s *scythe) removeCarFile(ctx context.Context, adCid cid.Cid) error {
	r := s.reaper
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
	s.stats.CarsRemoved++
	s.stats.CarsDataSize += file.Size
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

func (s *scythe) loadGCState(ctx context.Context) (GCState, error) {
	if s.dstore == nil {
		return GCState{}, nil
	}

	gcStateData, err := s.dstore.Get(ctx, datastore.NewKey(dsGCStateKey))
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

func (s *scythe) saveGCState(ctx context.Context, gcState GCState) error {
	if !s.reaper.commit || s.dstore == nil {
		return nil
	}

	data, err := json.Marshal(&gcState)
	if err != nil {
		return fmt.Errorf("cannot encode gc state data: %w", err)
	}

	key := datastore.NewKey(dsGCStateKey)
	err = s.dstore.Put(ctx, key, data)
	if err != nil {
		return err
	}

	return s.dstore.Sync(ctx, key)
}

func (s *scythe) deleteEntries(ctx context.Context, adCid cid.Cid) error {
	var mhCount int
	var err error
	var source string
	if s.reaper.carReader != nil {
		mhCount, err = s.removeEntriesWithCar(ctx, adCid)
		s.stats.IndexesRemoved += mhCount
		if err != nil {
			if errors.Is(err, errIndexerWrite) {
				return err
			}
			log.Warnw("Cannot get advertisement from car store, will try publisher", "err", err)
		}
		source = "CAR"
	}
	if s.reaper.carReader == nil || err != nil {
		mhCount, err = s.removeEntriesWithPublisher(ctx, adCid)
		s.stats.IndexesRemoved += mhCount
		if err != nil {
			if errors.Is(err, errIndexerWrite) {
				return err
			}
			log.Warnw("Cannot get advertisement from publisher, not deleting entries", "err", err)
			return errors.New("cannot get index data from advertisement to delete")
		}
		source = "synced from publisher"
	}
	log.Infow("Deleted indexes for removed contextID", "count", mhCount, "total", s.stats.IndexesRemoved, "source", source, "adsProcessed", s.stats.AdsProcessed)
	return nil
}

func (s *scythe) removeEntriesWithCar(ctx context.Context, adCid cid.Cid) (int, error) {
	// Create a context to cancel reading entries.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adBlock, err := s.reaper.carReader.Read(ctx, adCid, false)
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
		ProviderID:    s.providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	commit := s.reaper.commit
	indexer := s.reaper.indexer

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
		if commit {
			if err = indexer.Remove(value, chunk.Entries...); err != nil {
				return mhCount, fmt.Errorf("%w: %w", errIndexerWrite, err)
			}
		}
		mhCount += len(chunk.Entries)
	}

	return mhCount, nil
}

func (s *scythe) removeEntriesWithPublisher(ctx context.Context, adCid cid.Cid) (int, error) {
	_, err := s.sub.SyncAdChain(ctx, s.publisher, dagsync.WithHeadAdCid(adCid), dagsync.ScopedDepthLimit(int64(1)))
	if err != nil {
		return 0, err
	}
	ad, err := s.loadAd(adCid)
	if err != nil {
		return 0, fmt.Errorf("failed to load advertisement %s: %w", adCid.String(), err)
	}
	if ad.Entries == nil || ad.Entries == schema.NoEntries {
		log.Errorw("Advertisement expected to have entries, but has none", "adCid", adCid, "provider", s.providerID)
		return 0, nil
	}
	entsCid := ad.Entries.(cidlink.Link).Cid

	value := indexer.Value{
		ProviderID:    s.providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	commit := s.reaper.commit
	indexer := s.reaper.indexer
	var mhCount int

	for entsCid != cid.Undef {
		if err = s.sub.SyncEntries(ctx, s.publisher, entsCid); err != nil {
			return mhCount, fmt.Errorf("Cannot sync entries from publisher: %w", err)
		}
		chunk, err := s.loadEntryChunk(entsCid)
		if err != nil {
			return mhCount, fmt.Errorf("failed to load first entry chunk: %w", err)
		}
		if commit {
			if err = indexer.Remove(value, chunk.Entries...); err != nil {
				return mhCount, fmt.Errorf("%w: %w", errIndexerWrite, err)
			}
		}
		mhCount += len(chunk.Entries)

		if chunk.Next == nil {
			break
		}
		entsCid = chunk.Next.(cidlink.Link).Cid
	}
	return mhCount, nil
}

func createDatastore(dir string) (datastore.Batching, error) {
	err := fsutil.DirWritable(dir)
	if err != nil {
		return nil, err
	}
	ds, err := leveldb.NewDatastore(dir, nil)
	if err != nil {
		return nil, err
	}
	return ds, nil
}

func dstoreDirName(publisherID peer.ID) string {
	return fmt.Sprint("gc-data-", publisherID.String())
}

func dstoreArchiveName(publisherID peer.ID) string {
	return dstoreDirName(publisherID) + ".tar.gz"
}

func (s *scythe) archiveDatastore(ctx context.Context, dstoreDir string) error {
	if !s.reaper.commit {
		return nil
	}
	if s.reaper.fileStore == nil {
		log.Warn("Filestore not available to save gc datastore to")
		return nil
	}
	tarName := dstoreArchiveName(s.publisher.ID)
	parent := filepath.Dir(dstoreDir)
	tarPath := filepath.Join(parent, tarName)

	err := targz.Create(dstoreDir, tarPath)
	if err != nil {
		return fmt.Errorf("failed to archive gc datastore: %w", err)
	}

	// Replace the old gc data archive with the new one.
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	fileInfo, err := s.reaper.fileStore.Put(ctx, tarName, f)
	f.Close()
	if err != nil {
		return fmt.Errorf("failed to store new gc data archive: %w", err)
	}
	if err = os.Remove(tarPath); err != nil {
		log.Errorw("Failed to remove local datastore archive file", "err", err)
	}
	log.Infow("Stored datastore archive in filestore", "name", tarName, "size", fileInfo.Size)

	return nil
}

func unarchiveDatastore(ctx context.Context, publisherID peer.ID, fileStore filestore.Interface, parentDir string) error {
	tarName := dstoreArchiveName(publisherID)
	log.Debugw("Datastore directory does not exist, fetching from archive", "name", tarName)

	err := fsutil.DirWritable(parentDir)
	if err != nil {
		return err
	}

	fileInfo, rc, err := fileStore.Get(ctx, tarName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cannot retrieve datastore archive from filestore: %w", err)
	}
	defer rc.Close()

	err = targz.ExtractReader(rc, parentDir)
	if err != nil {
		return fmt.Errorf("failed to extract datastore archive: %w", err)
	}

	log.Infow("Extracted datastore archive from filestore", "name", tarName, "size", fileInfo.Size, "targetDir", parentDir)
	return nil
}

func (s *scythe) loadAd(c cid.Cid) (schema.Advertisement, error) {
	adn, err := s.loadNode(c, schema.AdvertisementPrototype)
	if err != nil {
		return schema.Advertisement{}, err
	}
	ad, err := schema.UnwrapAdvertisement(adn)
	if err != nil {
		return schema.Advertisement{}, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	return *ad, nil
}

func (s *scythe) loadEntryChunk(c cid.Cid) (*schema.EntryChunk, error) {
	node, err := s.loadNode(c, schema.EntryChunkPrototype)
	if err != nil {
		return nil, err
	}
	return schema.UnwrapEntryChunk(node)
}

func (s *scythe) loadNode(c cid.Cid, prototype ipld.NodePrototype) (ipld.Node, error) {
	val, err := s.dstoreTmp.Get(context.Background(), datastore.NewKey(c.String()))
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
