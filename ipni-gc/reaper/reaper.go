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
	"strings"
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
	commit      bool
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

	return &Reaper{
		carDelete:   opts.carDelete,
		carReader:   carReader,
		commit:      opts.commit,
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
		if r.delNotFound {
			return r.removePublisher(ctx, providerID, providerID)
		}
		return ErrProviderNotFound
	}
	if pinfo.Publisher == nil {
		return errors.New("provider has no publisher")
	}
	publisher := *pinfo.Publisher

	// Check that parent directory for the gc datastore directory is writable.
	if err = fsutil.DirWritable(r.dsDir); err != nil {
		return err
	}
	// If the gc datastore dir does not already exist, try to get archive from
	// filestore.
	dstoreDir := filepath.Join(r.dsDir, dstoreDirName(publisher.ID))
	_, err = os.Stat(dstoreDir)
	if errors.Is(err, fs.ErrNotExist) && r.fileStore != nil {
		if err = unarchiveDatastore(ctx, publisher.ID, r.fileStore, r.dsDir); err != nil {
			return fmt.Errorf("failed to retrieve datastore from archive: %w", err)
		}
	}
	// Check that the extracted archive directory is readable, and if it does
	// not exist create a new gc datastore directory.
	dstore, err := createDatastore(dstoreDir)
	if err != nil {
		return fmt.Errorf("failed to create ipni-gc datastore: %w", err)
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
		providerID: pinfo.AddrInfo.ID,
		publisher:  publisher,
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
		return "", errors.New("provider has no publisher")
	}
	return dstoreArchiveName(pinfo.Publisher.ID), nil
}

func (r *Reaper) removePublisher(ctx context.Context, providerID, publisherID peer.ID) error {
	startTime := time.Now()

	if r.carReader == nil {
		return ErrNoCarReader
	}

	adCid, err := r.readHeadFile(ctx, publisherID)
	if err != nil {
		return fmt.Errorf("cannot read head advertisement: %w", err)
	}

	adProviderID, err := r.providerFromCar(ctx, adCid)
	if err != nil {
		return fmt.Errorf("cannot read provider from car: %w", err)
	}

	if providerID == "" {
		providerID = adProviderID
	} else if providerID != adProviderID {
		return errors.New("requested provider does not match provider in advertisement")
	}

	// Delete gc-datastore archive.
	name := dstoreArchiveName(publisherID)
	err = r.fileStore.Delete(ctx, name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Errorw("Cannot delete datastore archive for provider", "err", err, "name", name)
	}
	// Delete gc-datastore.
	name = filepath.Join(r.dsDir, dstoreDirName(publisherID))
	err = os.RemoveAll(name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Errorw("Cannot remove datastore directory for provider", "err", err, "dir", name)
	}
	// Delete temporary gc-datastore.
	if r.dsTmpDir != "" {
		name = filepath.Join(r.dsTmpDir, "gc-tmp-"+publisherID.String())
		err = os.RemoveAll(name)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Errorw("Cannot remove datastore directory for provider", "err", err, "dir", name)
		}
	}
	err = nil

	var stats GCStats
	var newHead cid.Cid
	var cleanErr error

	for adCid != cid.Undef {
		prevAdCid, mhCount, err := r.cleanCarIndexes(ctx, adCid, providerID)
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

	headName := publisherID.String() + carstore.HeadFileSuffix
	if adCid == cid.Undef {
		err = r.fileStore.Delete(ctx, headName)
		if err != nil {
			err = fmt.Errorf("failed to delete head file: %w", err)
		}
		stats.TimeElapsed = time.Since(startTime)
		log.Infow("Finished GC for removed provider", "provider", providerID, "stats", stats.String())
	} else if newHead != cid.Undef {
		// Did not complete. Save head where GC left off.
		_, err = r.fileStore.Put(ctx, headName, strings.NewReader(newHead.String()))
		if err != nil {
			err = fmt.Errorf("failed to update head file: %w", err)
		}
		stats.TimeElapsed = time.Since(startTime)
		log.Infow("Incomplete GC for removed provider", "provider", providerID, "stats", stats.String())
	}
	if err != nil {
		if cleanErr != nil {
			log.Error(err.Error())
		} else {
			cleanErr = err
		}
	}

	r.AddStats(stats)
	return cleanErr
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

func (r *Reaper) cleanCarIndexes(ctx context.Context, adCid cid.Cid, providerID peer.ID) (cid.Cid, int, error) {
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

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	commit := r.commit
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
		if commit {
			if err = indexer.Remove(value, chunk.Entries...); err != nil {
				return cid.Undef, mhCount, fmt.Errorf("%w: %w", errIndexerWrite, err)
			}
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
	log.Infow("Deleting CAR file", "name", carPath, "size", file.Size)
	if r.commit {
		if err = r.fileStore.Delete(ctx, carPath); err != nil {
			return 0, fmt.Errorf("failed to remove CAR file: %w", err)
		}
	}
	return file.Size, nil
}

func (r *Reaper) readHeadFile(ctx context.Context, publisherID peer.ID) (cid.Cid, error) {
	headName := publisherID.String() + carstore.HeadFileSuffix
	_, rc, err := r.fileStore.Get(ctx, headName)
	if err != nil {
		return cid.Undef, err
	}

	cidData, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return cid.Undef, err
	}

	return cid.Decode(string(cidData))
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

	defer func() {
		if s.stats.IndexAdsRemoved != 0 {
			s.dstore.Sync(ctx, datastore.NewKey(dsRmPrefix))
		}
	}()

	removedCtxSet := make(map[string]struct{})
	remaining := make(map[string][]cid.Cid)

	for adCid := latestAdCid; adCid != gcState.LastProcessedAdCid; {
		ad, err := s.loadAd(adCid)
		if err != nil {
			return fmt.Errorf("failed to load advertisement %s: %w", adCid.String(), err)
		}
		contextID := base64.StdEncoding.EncodeToString(ad.ContextID)
		if ad.IsRm {
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
		} else if ad.Entries == nil || ad.Entries == schema.NoEntries {
			log.Debugw("Processing no-content ad", "adCid", adCid)
			// Delete CAR file of empty ad.
			if err = s.deleteCarFile(ctx, adCid); err != nil {
				return err
			}
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

		if ad.PreviousID == nil {
			adCid = cid.Undef
		} else {
			adCid = ad.PreviousID.(cidlink.Link).Cid
		}
	}
	log.Debugw("Done processing advertisements", "processed", s.stats.AdsProcessed, "removed", s.stats.IndexAdsRemoved)

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

func (s *scythe) reapPrevRemaining(ctx context.Context, contextID string) error {
	prefix := dsContextPrefix(contextID)
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

	if len(ents) == 0 {
		return nil
	}

	log.Debugw("Deleting indexes from previously processed ads that have removed contextID", "ads", len(ents))
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
		if err = s.saveRemoved(ctx, adCid); err != nil {
			return err
		}
		if commit {
			if err = s.dstore.Delete(ctx, datastore.NewKey(key)); err != nil {
				return err
			}
		}
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
	if err != nil && s.reaper.entsFromPub {
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
		log.Infow("Removed indexes in removed ad", "adCid", adCid, "count", newRemoved, "total", s.stats.IndexesRemoved, "source", source, "adsProcessed", s.stats.AdsProcessed)
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

	commit := s.reaper.commit
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
		if commit {
			if err = indexer.Remove(value, chunk.Entries...); err != nil {
				return fmt.Errorf("%w: %w", errIndexerWrite, err)
			}
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
		log.Errorw("Advertisement expected to have entries, but has none", "adCid", adCid, "provider", s.providerID)
		return nil
	}
	entsCid := ad.Entries.(cidlink.Link).Cid

	providerID, err := peer.Decode(ad.Provider)
	if err != nil {
		log.Errorw("Cannot get provider from advertisement", "err", err)
		providerID = s.providerID
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     ad.ContextID,
		MetadataBytes: ad.Metadata,
	}

	commit := s.reaper.commit
	indexer := s.reaper.indexer

	for entsCid != cid.Undef {
		if err = s.sub.SyncEntries(ctx, s.publisher, entsCid); err != nil {
			return fmt.Errorf("cannot sync entries from publisher: %w", err)
		}
		chunk, err := s.loadEntryChunk(entsCid)
		if err != nil {
			return fmt.Errorf("failed to load first entry chunk: %w", err)
		}
		if commit {
			if err = indexer.Remove(value, chunk.Entries...); err != nil {
				log.Errorw("Failed to remove indexes from valuestore, retrying", "err", err, "indexes", len(chunk.Entries))
				time.Sleep(100 * time.Millisecond)
				if err = indexer.Remove(value, chunk.Entries...); err != nil {
					return fmt.Errorf("%w: %w", errIndexerWrite, err)
				}
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

	fileInfo, rc, err := fileStore.Get(ctx, tarName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			log.Debug("Datastore archive not found, will create new datastore")
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
