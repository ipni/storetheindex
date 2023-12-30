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

	"github.com/gammazero/deque"
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

var (
	ErrProviderNotFound = errors.New("provider not found")
	ErrNoCarReader      = errors.New("car reader not available")
)

var errIndexerWrite = errors.New("delete from valuestore failed")

type GCStats struct {
	AdsProcessed    int
	CarsDataSize    int64
	CarsRemoved     int
	CtxIDsKept      int
	CtxIDsRemoved   int
	EmptyAds        int
	IndexAdsKept    int
	IndexAdsRemoved int
	IndexesRemoved  int
	RemovalAds      int
	ReusedCtxIDs    int
	TimeElapsed     time.Duration
}

func (s GCStats) String() string {
	return fmt.Sprint(
		"AdsProcessed:", s.AdsProcessed,
		" CarsDataSize:", s.CarsDataSize,
		" CarsRemoved:", s.CarsRemoved,
		" CtxIDsKept:", s.CtxIDsKept,
		" CtxIDsRemoved:", s.CtxIDsRemoved,
		" EmptyAds:", s.EmptyAds,
		" IndexAdsKept:", s.IndexAdsKept,
		" IndexAdsRemoved:", s.IndexAdsRemoved,
		" IndexesRemoved:", s.IndexesRemoved,
		" RemovalAds:", s.RemovalAds,
		" ReusedCtxIDs:", s.ReusedCtxIDs,
		" TimeElapsed:", s.TimeElapsed,
	)
}

type Reaper struct {
	carDelete   bool
	carReader   *carstore.CarReader
	delNotFound bool
	dsDir       string
	dsTmpDir    string
	entsFromPub bool
	fileStore   filestore.Interface
	host        host.Host
	hostOwner   bool
	httpTimeout time.Duration
	indexer     indexer.Interface
	pcache      *pcache.ProviderCache
	segmentSize int
	stats       GCStats
	statsMutex  sync.Mutex
	syncSegSize int
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

type adInfo struct {
	cid       cid.Cid
	contextID []byte
	empty     bool
	isRm      bool
}

func dsContextPrefix(contextID string) string {
	return fmt.Sprintf("/ctx/%s/", contextID)
}

const (
	dsGCStateKey = "/state"
	dsRmPrefix   = "/rm/"
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
	} else if !opts.entsFromPub {
		// No car reader and cannot get from pub, so no way to get entries.
		return nil, ErrNoCarReader
	}

	if opts.pcache == nil {
		opts.pcache, err = pcache.New(pcache.WithPreload(false),
			pcache.WithRefreshInterval(0),
			pcache.WithSourceURL("http://localhost:3000"))
		if err != nil {
			return nil, err
		}
	}

	if opts.dstoreTmpDir == "" {
		opts.dstoreTmpDir = os.TempDir()
	}

	return &Reaper{
		carDelete:   opts.carDelete,
		carReader:   carReader,
		delNotFound: opts.deleteNotFound,
		dsDir:       opts.dstoreDir,
		dsTmpDir:    opts.dstoreTmpDir,
		entsFromPub: opts.entsFromPub,
		fileStore:   fileStore,
		host:        opts.p2pHost,
		hostOwner:   hostOwner,
		httpTimeout: opts.httpTimeout,
		indexer:     idxr,
		pcache:      opts.pcache,
		segmentSize: opts.segmentSize,
		syncSegSize: opts.syncSegSize,
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
	r.stats.EmptyAds += a.EmptyAds
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
		if r.delNotFound {
			return r.removeProvider(ctx, providerID)
		}
		return ErrProviderNotFound
	}
	if pinfo.Publisher == nil {
		return errors.New("provider has no publisher")
	}

	dstore, err := r.openDatastore(ctx, providerID)
	if err != nil {
		return err
	}
	defer func() {
		if dstore != nil {
			dstore.Close()
		}
	}()

	// Create datastore for temporary ad data.
	if err = fsutil.DirWritable(r.dsTmpDir); err != nil {
		return err
	}
	tmpDir := filepath.Join(r.dsTmpDir, "gc-tmp-"+providerID.String())
	dstoreTmp, err := createDatastore(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create temporary datastore: %w", err)
	}
	defer dstoreTmp.Close()

	// Create ipni dagsync Subscriber for the provider.
	sub, err := r.makeSubscriber(dstoreTmp)
	if err != nil {
		return fmt.Errorf("failed to start dagsync subscriber: %w", err)
	}
	defer sub.Close()

	s := &scythe{
		reaper:     r,
		dstore:     dstore,
		dstoreTmp:  dstoreTmp,
		providerID: pinfo.AddrInfo.ID,
		publisher:  *pinfo.Publisher,
		sub:        sub,
		tmpDir:     tmpDir,
	}

	startTime := time.Now()

	err = s.reap(ctx, pinfo.LastAdvertisement)
	if err != nil {
		log.Errorw("Could not process advertisement chain for GC", "err", err)
	} else {
		defer os.RemoveAll(tmpDir)
	}

	err = s.reapRemoved(ctx)
	if err != nil {
		return err
	}

	s.stats.TimeElapsed = time.Since(startTime)
	log.Infow("Finished GC for provider", "provider", providerID, "stats", s.stats.String())
	r.AddStats(s.stats)

	if dstore == nil {
		return nil
	}
	dstore.Close()
	dstore = nil
	return s.archiveDatastore(ctx)
}

func (r *Reaper) datastoreExists(ctx context.Context, providerID peer.ID) (bool, error) {
	dir := filepath.Join(r.dsDir, dstoreDirName(providerID))
	fi, err := os.Stat(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
		_, err = r.fileStore.Head(ctx, ArchiveName(providerID))
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return false, err
			}
			return false, nil
		}
		return true, nil
	}
	if !fi.IsDir() {
		return false, fmt.Errorf("not a directory: %s", dir)
	}
	return true, nil
}

func (r *Reaper) openDatastore(ctx context.Context, providerID peer.ID) (datastore.Batching, error) {
	// Check that parent directory for the gc datastore directory is writable.
	err := fsutil.DirWritable(r.dsDir)
	if err != nil {
		return nil, err
	}
	// If the gc datastore dir does not already exist, try to get archive from
	// filestore.
	dstoreDir := filepath.Join(r.dsDir, dstoreDirName(providerID))
	_, err = os.Stat(dstoreDir)
	if errors.Is(err, fs.ErrNotExist) && r.fileStore != nil {
		if err = r.unarchiveDatastore(ctx, providerID); err != nil {
			return nil, fmt.Errorf("failed to retrieve datastore from archive: %w", err)
		}
	}
	// Check that the extracted archive directory is readable, and if it does
	// not exist create a new gc datastore directory.
	dstore, err := createDatastore(dstoreDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create ipni-gc datastore: %w", err)
	}
	return dstore, nil
}

func (r *Reaper) removeProvider(ctx context.Context, providerID peer.ID) error {
	startTime := time.Now()

	exists, err := r.datastoreExists(ctx, providerID)
	if err != nil {
		return err
	}
	if exists {
		dstore, err := r.openDatastore(ctx, providerID)
		if err != nil {
			return err
		}
		defer dstore.Close()

		s := &scythe{
			reaper:     r,
			dstore:     dstore,
			providerID: providerID,
		}

		err = s.reapAllPrevRemaining(ctx)
		if err != nil {
			return fmt.Errorf("failed to remove remaining ads from previous gc: %w", err)
		}

		err = s.reapRemoved(ctx)
		if err != nil {
			return err
		}

		dstore.Close()

		// Delete gc-datastore archive.
		name := ArchiveName(providerID)
		err = r.fileStore.Delete(ctx, name)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Errorw("Cannot delete datastore archive for provider", "err", err, "name", name)
		}
		// Delete gc-datastore.
		dstoreDir := filepath.Join(r.dsDir, dstoreDirName(providerID))
		err = os.RemoveAll(dstoreDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Errorw("Cannot remove datastore directory for provider", "err", err, "dir", dstoreDir)
		}

		r.AddStats(s.stats)
	}

	// Delete temporary gc-datastore.
	name := filepath.Join(r.dsTmpDir, "gc-tmp-"+providerID.String())
	err = os.RemoveAll(name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Errorw("Cannot remove datastore directory for provider", "err", err, "dir", name)
	}

	if r.carReader == nil {
		return ErrNoCarReader
	}

	carWriter, err := carstore.NewWriter(nil, r.fileStore)
	if err != nil {
		return err
	}

	adCid, err := r.carReader.ReadHead(ctx, providerID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// OK if head file does not exist. No CAR files to clean up.
			return nil
		}
		if err = carWriter.DeleteHead(ctx, providerID); err != nil {
			log.Errorw("Failed to delete head file for provider", "err", err, "provider", providerID)
		}
		return fmt.Errorf("cannot read head advertisement for provider %s: %w", providerID, err)
	}

	adProviderID, err := r.providerFromCar(ctx, adCid)
	if err != nil {
		return fmt.Errorf("cannot read provider from car: %w", err)
	}
	// If providerID is different from the one in the advertisement, then it
	// means the publisher ID was given as providerID, since that is what head
	// files were previously named with. The real providerID comes from the CAR
	// file, so check that the provider does not still exist.
	if adProviderID != providerID {
		providerID = adProviderID
		pinfo, err := r.pcache.Get(ctx, providerID)
		if err != nil {
			return err
		}
		if pinfo != nil {
			return fmt.Errorf("provider %s still exists", providerID)
		}
	}

	var stats GCStats
	var newHead cid.Cid
	var cleanErr error

	for adCid != cid.Undef {
		prevAdCid, mhCount, err := r.cleanCarIndexes(ctx, adCid)
		stats.IndexesRemoved += mhCount
		if err != nil {
			cleanErr = fmt.Errorf("stopping gc due to error removing entries in CAR: %w", err)
			break
		}
		size, err := r.deleteCarFile(ctx, adCid)
		if err != nil {
			cleanErr = fmt.Errorf("cannot delete car file: %w", err)
			break
		}
		if size != 0 {
			stats.CarsRemoved++
			stats.CarsDataSize += size
		}
		newHead = adCid
		adCid = prevAdCid
	}

	if adCid == cid.Undef {
		if err = carWriter.DeleteHead(ctx, providerID); err != nil {
			err = fmt.Errorf("failed to delete head file: %w", err)
		}
		stats.TimeElapsed = time.Since(startTime)
		log.Infow("Finished GC for removed provider", "provider", providerID, "stats", stats.String())
	} else if newHead != cid.Undef {
		// Did not complete. Save head where GC left off.
		_, err = carWriter.WriteHead(ctx, newHead, providerID)
		if err != nil {
			err = fmt.Errorf("failed to update head file: %w", err)
		}
		stats.TimeElapsed = time.Since(startTime)
		log.Infow("Incomplete GC for removed provider", "provider", providerID, "stats", stats.String())
	}
	if err != nil {
		if cleanErr != nil {
			log.Error(err.Error())
			err = cleanErr
		}
	}

	r.AddStats(stats)
	return err
}

func (r *Reaper) providerFromCar(ctx context.Context, adCid cid.Cid) (peer.ID, error) {
	adBlock, err := r.carReader.Read(ctx, adCid, true)
	if err != nil {
		return "", err
	}
	ad, err := adBlock.Advertisement()
	if err != nil {
		return "", err
	}
	return peer.Decode(ad.Provider)
}

func (r *Reaper) cleanCarIndexes(ctx context.Context, adCid cid.Cid) (cid.Cid, int, error) {
	// Create a context to cancel the carReader reading entries.
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	adBlock, err := r.carReader.Read(readCtx, adCid, false)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("cannot read car file: %w", err)
	}

	ad, err := adBlock.Advertisement()
	if err != nil {
		log.Errorw("Failed to decode advertisement from car file", "err", err)
		return cid.Undef, 0, nil
	}

	var prevAdCid cid.Cid
	if ad.PreviousID != nil {
		prevAdCid = ad.PreviousID.(cidlink.Link).Cid
	}

	if adBlock.Entries == nil {
		return prevAdCid, 0, nil
	}

	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return cid.Undef, 0, fmt.Errorf("cannot get provider from advertisement: %w", err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	indexer := r.indexer

	var mhCount int
	for entryBlock := range adBlock.Entries {
		if entryBlock.Err != nil {
			log.Errorw("Error reading entries block", "err", err)
			continue
		}
		chunk, err := entryBlock.EntryChunk()
		if err != nil {
			log.Errorw("Failed to decode entry chunk from car file data", "err", err)
			continue
		}
		if len(chunk.Entries) == 0 {
			continue
		}
		if err = indexer.Remove(value, chunk.Entries...); err != nil {
			return cid.Undef, mhCount, fmt.Errorf("%w: %w", errIndexerWrite, err)
		}
		mhCount += len(chunk.Entries)
	}

	return prevAdCid, mhCount, nil
}

func (r *Reaper) deleteCarFile(ctx context.Context, adCid cid.Cid) (int64, error) {
	if r.carReader == nil || !r.carDelete {
		return 0, nil
	}
	carPath := r.carReader.CarPath(adCid)
	file, err := r.fileStore.Head(ctx, carPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if err = r.fileStore.Delete(ctx, carPath); err != nil {
		return 0, fmt.Errorf("failed to remove CAR file: %w", err)
	}
	log.Infow("Deleted CAR file", "name", carPath, "size", file.Size)
	return file.Size, nil
}

func (r *Reaper) makeSubscriber(dstoreTmp datastore.Batching) (*dagsync.Subscriber, error) {
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

	return dagsync.NewSubscriber(r.host, dstoreTmp, linksys, r.topic,
		dagsync.HttpTimeout(r.httpTimeout),
		dagsync.SegmentDepthLimit(int64(r.syncSegSize)))
}

func (s *scythe) generalDagsyncBlockHook(_ peer.ID, adCid cid.Cid, actions dagsync.SegmentSyncActions) {
	// The only kind of block we should get by loading CIDs here should be
	// Advertisement.
	//
	// Because:
	//  - the default subscription selector only selects advertisements.
	//  - explicit Ingester.Sync only selects advertisement.
	//  - entries are synced with an explicit selector separate from
	//    advertisement syncs and should use dagsync.ScopedBlockHook to
	//    override this hook and decode chunks instead.
	//
	// Therefore, we only attempt to load advertisements here and signal
	// failure if the load fails.
	ad, err := s.loadAd(adCid)
	if err != nil {
		actions.FailSync(err)
	} else if ad.PreviousID != nil {
		actions.SetNextSyncCid(ad.PreviousID.(cidlink.Link).Cid)
	} else {
		actions.SetNextSyncCid(cid.Undef)
	}
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
		dagsync.ScopedBlockHook(s.generalDagsyncBlockHook),
	)
	if err != nil {
		return fmt.Errorf("failed to sync advertisement chain: %w", err)
	}
	log.Debug("Synced unprocessed portion of advertisement chain from publisher")

	defer func() {
		if s.stats.IndexAdsRemoved != 0 {
			s.dstore.Sync(ctx, datastore.NewKey(dsRmPrefix))
		}
	}()

	segSize := s.reaper.segmentSize
	segment := deque.New[adInfo](segSize, segSize)

	for segEnd := gcState.LastProcessedAdCid; segEnd != latestAdCid; {
		for adCid := latestAdCid; adCid != segEnd; {
			ad, err := s.loadAd(adCid)
			if err != nil {
				return fmt.Errorf("failed to load advertisement %s: %w", adCid.String(), err)
			}
			if segment.Len() == segSize {
				segment.PopFront()
			}
			segment.PushBack(adInfo{
				cid:       adCid,
				contextID: ad.ContextID,
				empty:     ad.Entries == nil || ad.Entries == schema.NoEntries,
				isRm:      ad.IsRm,
			})
			if ad.PreviousID == nil {
				break
			}
			adCid = ad.PreviousID.(cidlink.Link).Cid
			if adCid == cid.Undef {
				// This should never happen, unless the provider has a
				// different chain that it did previously.
				log.Errorw("Did not find last provessed advertisement in chain", "lastProcessed", segEnd, "chainEnd", adCid)
				break
			}
		}

		segEnd = segment.Front().cid

		err = s.reapSegment(ctx, segment)
		if err != nil {
			return err
		}

		// Update GC state.
		gcState.LastProcessedAdCid = segEnd
		if err = s.saveGCState(ctx, gcState); err != nil {
			return err
		}
	}
	return nil
}

func (s *scythe) reapSegment(ctx context.Context, segment *deque.Deque[adInfo]) error {
	log.Infow("Processing advertisements segment", "size", segment.Len())

	removedCtxSet := make(map[string]struct{})
	remaining := make(map[string][]cid.Cid)
	var err error

	for segment.Len() != 0 {
		ad := segment.Front()
		segment.PopFront()
		adCid := ad.cid
		contextID := base64.StdEncoding.EncodeToString(ad.contextID)
		if ad.isRm {
			log.Debugw("Processing removal ad", "adCid", adCid)
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
			// Do not keep car files for removal ads.
			if err = s.deleteCarFile(ctx, adCid); err != nil {
				return err
			}
		} else if ad.empty {
			log.Debugw("Processing no-content ad", "adCid", adCid)
			// Delete CAR file of empty ad.
			if err = s.deleteCarFile(ctx, adCid); err != nil {
				return err
			}
			s.stats.EmptyAds++
		} else {
			_, ok := removedCtxSet[contextID]
			if ok {
				log.Debugw("Marking index ad for removal", "adCid", adCid)
				if err = s.saveRemoved(ctx, adCid); err != nil {
					return err
				}
			} else {
				log.Debugw("Keeping index content", "adCid", adCid)
				s.stats.IndexAdsKept++
				// Append ad cid to list of remaining for this context ID.
				remaining[contextID] = append(remaining[contextID], adCid)
			}
		}
		s.stats.AdsProcessed++
	}

	// Record which ads remain undeleted.
	if err = s.saveRemaining(ctx, remaining); err != nil {
		return err
	}
	s.stats.CtxIDsKept += len(remaining)
	s.stats.CtxIDsRemoved += len(removedCtxSet)

	log.Infow("Done processing advertisements segment", "stats", s.stats.String())
	return nil
}

func (s *scythe) saveRemoved(ctx context.Context, adCid cid.Cid) error {
	// Put removed ad CID in datastore for deleting later.
	err := s.dstore.Put(ctx, datastore.NewKey(dsRmPrefix+adCid.String()), []byte{})
	if err != nil {
		return fmt.Errorf("failed to write removed ad to datastore: %w", err)
	}
	s.stats.IndexAdsRemoved++
	return nil
}

func (s *scythe) reapRemoved(ctx context.Context) error {
	log.Infow("Removing index content for all removed advertisements")

	q := query.Query{
		Prefix:   dsRmPrefix,
		KeysOnly: true,
	}
	results, err := s.dstore.Query(ctx, q)
	if err != nil {
		return err
	}

	defer s.dstore.Sync(ctx, datastore.NewKey(dsRmPrefix))

	for r := range results.Next() {
		if r.Error != nil {
			results.Close()
			return fmt.Errorf("error querying removed ads for provider %s: %w", s.providerID, err)
		}
		adCid, err := cid.Decode(path.Base(r.Entry.Key))
		if err != nil {
			log.Errorw("Cannot decode removed advertisement cid", "err", err)
		}

		// Ad's context ID is removed, so delete all multihashes for ad.
		if err = s.removeEntries(ctx, adCid); err != nil {
			return fmt.Errorf("could not delete advertisement content: %w", err)
		}

		// Do not keep CAR file with removed entries.
		if err = s.deleteCarFile(ctx, adCid); err != nil {
			return err
		}

		err = s.dstore.Delete(ctx, datastore.NewKey(r.Entry.Key))
		if err != nil {
			log.Errorw("Cannot delete removed advertisement cid", "err", err)
		}
	}
	results.Close()
	return nil
}

// reapAllPrevRemaining marks all previously remaining ads for removal.
func (s *scythe) reapAllPrevRemaining(ctx context.Context) error {
	return s.reapPrefixedAds(ctx, "/ctx/")
}

// reapPrevRemaining marks previously remaining ads for removeal that have the
// specified context ID.
func (s *scythe) reapPrevRemaining(ctx context.Context, contextID string) error {
	return s.reapPrefixedAds(ctx, dsContextPrefix(contextID))
}

func (s *scythe) reapPrefixedAds(ctx context.Context, prefix string) error {
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

	if len(ents) == 0 {
		return nil
	}

	log.Debugw("Deleting indexes from previously processed ads that have removed contextID", "ads", len(ents))
	for i := range ents {
		key := ents[i].Key
		adCid, err := cid.Decode(path.Base(key))
		if err != nil {
			log.Errorw("Cannot decode remaining advertisement cid", "err", err)
			if err = s.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
				return err
			}
			continue
		}
		if err = s.saveRemoved(ctx, adCid); err != nil {
			return err
		}
		if err = s.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
			return err
		}
	}

	return s.dstore.Sync(ctx, datastore.NewKey(prefix))
}

func (s *scythe) saveRemaining(ctx context.Context, remaining map[string][]cid.Cid) error {
	for contextID, adCids := range remaining {
		ctxPrefix := dsContextPrefix(contextID)
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

func (s *scythe) deleteCarFile(ctx context.Context, adCid cid.Cid) error {
	size, err := s.reaper.deleteCarFile(ctx, adCid)
	if err != nil {
		return err
	}
	if size != 0 {
		s.stats.CarsRemoved++
		s.stats.CarsDataSize += size
	}
	return nil
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
	if s.dstore == nil {
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

// removeEntries acquires the entries for the specified advertisement, either
// from a CAR file or from the publisher, and removes the corresponding
// indexes.
func (s *scythe) removeEntries(ctx context.Context, adCid cid.Cid) error {
	prevRemoved := s.stats.IndexesRemoved
	var err error
	source := "none"

	if s.reaper.carReader != nil {
		err = s.removeEntriesFromCar(ctx, adCid)
		if err != nil {
			if errors.Is(err, errIndexerWrite) {
				return err
			}
			log.Warnw("Cannot get advertisement from car store", "err", err, "adCid", adCid)
		} else {
			source = "CAR"
		}
	} else {
		err = ErrNoCarReader
	}
	if err != nil && s.reaper.entsFromPub && s.sub != nil {
		err = s.removeEntriesFromPublisher(ctx, adCid)
		if err != nil {
			if errors.Is(err, errIndexerWrite) {
				return err
			}
			log.Warnw("Cannot get advertisement from publisher, not deleting entries", "err", err, "adCid", adCid)
			return nil
		} else {
			source = "synced from publisher"
		}
	}
	newRemoved := s.stats.IndexesRemoved - prevRemoved
	if newRemoved != 0 {
		log.Infow("Removed indexes in removed ad", "adCid", adCid, "count", newRemoved, "total", s.stats.IndexesRemoved, "source", source)
	}
	return nil
}

func (s *scythe) removeEntriesFromCar(ctx context.Context, adCid cid.Cid) error {
	// Create a context to cancel the carReader reading entries.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	adBlock, err := s.reaper.carReader.Read(ctx, adCid, false)
	if err != nil {
		return err
	}

	if adBlock.Entries == nil {
		// This advertisement has no entries because they were removed later in
		// the chain and were not saved, or the publisher was not serving
		// content for the advertisement's entries CID when this CAR file was
		// created.
		return nil
	}

	ad, err := adBlock.Advertisement()
	if err != nil {
		return fmt.Errorf("failed to decode advertisement from car file: %w", err)
	}

	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return fmt.Errorf("cannot get provider from advertisement: %w", err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	indexer := s.reaper.indexer

	for entryBlock := range adBlock.Entries {
		if entryBlock.Err != nil {
			return entryBlock.Err
		}
		chunk, err := entryBlock.EntryChunk()
		if err != nil {
			log.Errorw("Failed to decode entry chunk from car file data", "err", err)
			continue
		}
		if len(chunk.Entries) == 0 {
			continue
		}
		if err = indexer.Remove(value, chunk.Entries...); err != nil {
			return fmt.Errorf("%w: %w", errIndexerWrite, err)
		}
		s.stats.IndexesRemoved += len(chunk.Entries)
	}

	return nil
}

func (s *scythe) removeEntriesFromPublisher(ctx context.Context, adCid cid.Cid) error {
	_, err := s.sub.SyncAdChain(ctx, s.publisher, dagsync.WithHeadAdCid(adCid), dagsync.ScopedDepthLimit(int64(1)))
	if err != nil {
		return err
	}
	ad, err := s.loadAd(adCid)
	if err != nil {
		return fmt.Errorf("failed to load advertisement %s: %w", adCid.String(), err)
	}
	if ad.Entries == nil || ad.Entries == schema.NoEntries {
		log.Errorw("Advertisement expected to have entries, but has none", "adCid", adCid, "provider", s.providerID, "publisher", s.publisher.ID)
		return nil
	}
	entsCid := ad.Entries.(cidlink.Link).Cid

	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		return fmt.Errorf("cannot get provider from advertisement %s: %w", adCid, err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	indexer := s.reaper.indexer

	for entsCid != cid.Undef {
		if err = s.sub.SyncEntries(ctx, s.publisher, entsCid); err != nil {
			return fmt.Errorf("cannot sync entries from publisher: %w", err)
		}
		chunk, err := s.loadEntryChunk(entsCid)
		if err != nil {
			return fmt.Errorf("failed to load first entry chunk: %w", err)
		}
		if err = indexer.Remove(value, chunk.Entries...); err != nil {
			log.Errorw("Failed to remove indexes from valuestore, retrying", "err", err, "indexes", len(chunk.Entries))
			time.Sleep(100 * time.Millisecond)
			if err = indexer.Remove(value, chunk.Entries...); err != nil {
				return fmt.Errorf("%w: %w", errIndexerWrite, err)
			}
		}
		s.stats.IndexesRemoved += len(chunk.Entries)

		if chunk.Next == nil {
			break
		}
		entsCid = chunk.Next.(cidlink.Link).Cid
	}
	return nil
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

func dstoreDirName(providerID peer.ID) string {
	return fmt.Sprint("gc-data-", providerID.String())
}

func ArchiveName(providerID peer.ID) string {
	return dstoreDirName(providerID) + ".tar.gz"
}

// archiveDatastore gets a gzipped tar archive of the provider gc datastore,
// from the filestore, and extracts the archive.
//
// If the dsDir is `/data/datastore-gc/` and the archive is
// gc-data-PID.tar.gz, then that archive is extracted as
// `/data/datastore-gc/gc-data-PID`.
func (r *Reaper) unarchiveDatastore(ctx context.Context, providerID peer.ID) error {
	tarName := ArchiveName(providerID)
	log.Debugw("Datastore directory does not exist, fetching from archive", "name", tarName)

	fileInfo, rc, err := r.fileStore.Get(ctx, tarName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Debug("Datastore archive not found, will create new datastore")
			return nil
		}
		return fmt.Errorf("cannot retrieve datastore archive from filestore: %w", err)
	}
	defer rc.Close()

	err = targz.ExtractReader(rc, r.dsDir)
	if err != nil {
		return fmt.Errorf("failed to extract datastore archive: %w", err)
	}

	log.Infow("Extracted datastore archive from filestore", "name", tarName, "size", fileInfo.Size, "targetDir", r.dsDir)
	return nil
}

// archiveDatastore creates a gzipped tar archive of the provider gc datastore
// and puts a copy of that into the filestore.
//
// If the provider gc datastore is `/data/datastore-gc/gc-data-PID` then
// create an archive of `gc-data-PId` named `gc-data-PID.tar.gz` and copy
// it to the filestore.
func (s *scythe) archiveDatastore(ctx context.Context) error {
	if s.reaper.fileStore == nil {
		log.Warn("Filestore not available to save gc datastore to")
		return nil
	}

	dstoreDir := filepath.Join(s.reaper.dsDir, dstoreDirName(s.providerID))
	tarName := ArchiveName(s.providerID)
	tarPath := filepath.Join(s.reaper.dsDir, tarName)

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
