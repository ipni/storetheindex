package ingest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/chanqueue"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/rate"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer/ingest")

// prefix used to track latest sync in datastore.
const (
	// syncPrefix identifies the latest sync for each provider.
	syncPrefix = "/sync/"
	// adProcessedPrefix identifies all processed advertisements.
	adProcessedPrefix = "/adProcessed/"
	// adProcessedFrozenPrefix identifies all advertisements processed while in
	// frozen mode. Used for unfreezing.
	adProcessedFrozenPrefix = "/adF/"
	// metricsUpdateInterval determines how ofter to update ingestion metrics.
	metricsUpdateInterval = time.Minute
)

type adProcessedEvent struct {
	publisher peer.ID
	// Head of the chain being processed.
	headAdCid cid.Cid
	// Actual adCid being processing.
	adCid cid.Cid
	// A non-nil value indicates failure to process the ad for adCid.
	err error
}

type adInfo struct {
	cid    cid.Cid
	resync bool
	skip   bool
}

// Ingester is a type that uses dagsync for the ingestion protocol.
//
// ## Advertisement Ingestion Constraints
//
// 1. If an Ad is processed, all older ads referenced by this ad (towards the
// start of the ad chain), have also been processes. For example, given some
// chain A <- B <- C, the indexer will never be in the state that it indexed A
// & C but not B.
//
// 2. An indexer will index an Ad chain, but will not make any guarantees about
// consistency in the presence of multiple ad chains for a given provider. For
// example if a provider publishes two ad chains at the same time chain1 and
// chain2 the indexer will apply whichever chain it learns about first, then
// apply the other chain.
//
// 3. An indexer will not index the same Ad twice. An indexer will be resilient
// to restarts. If the indexer goes down and comes back up it should not break
// constraint 1.
type Ingester struct {
	host    host.Host
	ds      datastore.Batching
	dsTmp   datastore.Batching
	lsys    ipld.LinkSystem
	indexer indexer.Interface

	closeOnce sync.Once

	sub         *dagsync.Subscriber
	syncTimeout time.Duration

	reg *registry.Registry

	// inEvents is used to send an adProcessedEvent to the distributeEvents
	// goroutine, when an advertisement in marked complete or having an error.
	inEvents chan adProcessedEvent

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of an adProcessedEvent to an onAdProcessed reader.
	outEventsChans map[peer.ID][]chan<- adProcessedEvent
	outEventsMutex sync.Mutex

	autoSyncDone      chan struct{}
	closePendingSyncs chan struct{}

	overwriteMirrorOnResync bool

	// A map of providers currently being processed. Used to detect if multiple
	// publishers are supplying ads for the same provide at the same time.
	providersBusy   map[peer.ID]struct{}
	providersBusyMu sync.Mutex

	// Used to stop watching for sync finished events from dagsync.
	cancelOnSyncFinished context.CancelFunc

	// Channels that workers read from.
	syncFinishedEvents <-chan dagsync.SyncFinished
	syncInProgressMu   sync.Mutex
	syncInProgress     map[peer.ID]*dagsync.SyncFinished

	// Context and cancel function used to cancel all workers.
	cancelWorkers context.CancelFunc
	workersCtx    context.Context

	// Worker pool resizing.
	nextWorkerNum  int
	stopWorker     chan struct{}
	waitForWorkers sync.WaitGroup
	workerPoolSize int

	// Multihash minimum length
	minKeyLen int

	mirror        adMirror
	mhsFromMirror atomic.Uint64

	// Ingest rates
	ingestRates *rate.Map

	skip500EntsErr atomic.Bool

	// metrics
	totalNonRmAds atomic.Int64
	totalRmAds    atomic.Int64
	workersActive atomic.Int32
}

// NewIngester creates a new Ingester that uses a dagsync Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr indexer.Interface, reg *registry.Registry, ds, dsTmp datastore.Batching) (*Ingester, error) {
	if cfg.IngestWorkerCount == 0 {
		return nil, errors.New("ingester worker count must be > 0")
	}

	ing := &Ingester{
		host:        h,
		ds:          ds,
		dsTmp:       dsTmp,
		lsys:        mkLinkSystem(dsTmp, reg),
		indexer:     idxr,
		syncTimeout: time.Duration(cfg.SyncTimeout),
		reg:         reg,
		inEvents:    make(chan adProcessedEvent, 1),

		autoSyncDone:      make(chan struct{}),
		closePendingSyncs: make(chan struct{}),

		overwriteMirrorOnResync: cfg.OverwriteMirrorOnResync,
		providersBusy:           make(map[peer.ID]struct{}),
		stopWorker:              make(chan struct{}),

		syncInProgress: make(map[peer.ID]*dagsync.SyncFinished),

		minKeyLen: cfg.MinimumKeyLength,

		ingestRates: rate.NewMap(),
	}

	ing.workersCtx, ing.cancelWorkers = context.WithCancel(context.Background())

	var err error
	ing.mirror, err = newMirror(cfg.AdvertisementMirror, ing.dsTmp)
	if err != nil {
		return nil, err
	}

	ing.skip500EntsErr.Store(cfg.Skip500EntriesError)

	// Create and start subscriber. This also registers the storage hook to
	// index data as it is received.
	sub, err := dagsync.NewSubscriber(h, ing.lsys,
		dagsync.AdsDepthLimit(int64(cfg.AdvertisementDepthLimit)),
		dagsync.EntriesDepthLimit(int64(cfg.EntriesDepthLimit)),
		dagsync.FirstSyncDepth(int64(cfg.FirstSyncDepth)),
		dagsync.WithLastKnownSync(ing.getLastKnownSync),
		dagsync.SegmentDepthLimit(int64(cfg.SyncSegmentDepthLimit)),
		dagsync.BlockHook(ing.generalDagsyncBlockHook),
		dagsync.RecvAnnounce(cfg.PubSubTopic,
			announce.WithAllowPeer(reg.Allowed),
			announce.WithFilterIPs(reg.FilterIPsEnabled()),
			announce.WithResend(cfg.ResendDirectAnnounce),
		),
		dagsync.MaxAsyncConcurrency(cfg.MaxAsyncConcurrency),
		dagsync.HttpTimeout(time.Duration(cfg.HttpSyncTimeout)),
		dagsync.RetryableHTTPClient(cfg.HttpSyncRetryMax, time.Duration(cfg.HttpSyncRetryWaitMin), time.Duration(cfg.HttpSyncRetryWaitMax)),
	)
	if err != nil {
		log.Errorw("Failed to start dagsync subscriber", "err", err)
		return nil, errors.New("ingester subscriber failed")
	}
	ing.sub = sub

	ing.syncFinishedEvents, ing.cancelOnSyncFinished = ing.sub.OnSyncFinished()

	ing.RunWorkers(cfg.IngestWorkerCount)

	// Start distributor to send SyncFinished messages to interested parties.
	go ing.distributeEvents()

	go ing.metricsUpdater()

	go ing.autoSync()

	return ing, nil
}

func (ing *Ingester) GetAllIngestRates() map[string]rate.Rate {
	return ing.ingestRates.GetAll()
}

func (ing *Ingester) GetIngestRate(peerID peer.ID) (rate.Rate, bool) {
	return ing.ingestRates.Get(string(peerID))
}

func (ing *Ingester) MultihashesFromMirror() uint64 {
	return ing.mhsFromMirror.Load()
}

func (ing *Ingester) Skip500EntriesError(skip bool) {
	ing.skip500EntsErr.Store(skip)
}

func (ing *Ingester) generalDagsyncBlockHook(_ peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
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
	if ad, err := ing.loadAd(c); err != nil {
		actions.FailSync(err)
	} else if ad.PreviousID != nil {
		actions.SetNextSyncCid(ad.PreviousID.(cidlink.Link).Cid)
	} else {
		actions.SetNextSyncCid(cid.Undef)
	}
}

// Close stops the ingester and all its workers.
func (ing *Ingester) Close() error {
	ing.cancelOnSyncFinished()

	// Tell workers to stop ingestion in progress.
	ing.cancelWorkers()

	// Close dagsync transport.
	err := ing.sub.Close()
	log.Info("dagsync subscriber stopped")

	// Dismiss any event readers.
	ing.outEventsMutex.Lock()
	for _, chans := range ing.outEventsChans {
		for _, ch := range chans {
			close(ch)
		}
	}
	ing.outEventsChans = nil
	ing.outEventsMutex.Unlock()

	ing.closeOnce.Do(func() {
		<-ing.autoSyncDone
		ing.waitForWorkers.Wait()
		log.Info("Workers stopped")
		close(ing.closePendingSyncs)
		log.Info("Pending sync processing stopped")

		// Stop the distribution goroutine.
		close(ing.inEvents)

		log.Info("Ingester stopped")
	})

	return err
}

func Unfreeze(unfrozen map[peer.ID]cid.Cid, dstore datastore.Datastore) error {
	// Unfreeze is not cancelable since unfrozen data can only be acquired once
	// from the registry.
	ctx := context.Background()

	// Remove all ads processed while frozen.
	err := removeProcessedFrozen(ctx, dstore)
	if err != nil {
		return fmt.Errorf("cannot remove processed ads: %w", err)
	}

	for pubID, frozenAt := range unfrozen {
		// If there was no previous frozen ad, then there was no latest ad
		// when the indexer was frozen.
		if frozenAt == cid.Undef {
			err = dstore.Delete(ctx, datastore.NewKey(syncPrefix+pubID.String()))
			if err != nil {
				log.Errorw("Unfreeze cannot delete last processed ad", "err", err)
			}
			continue
		}

		// Set last processed ad to previous frozen at ad.
		err = dstore.Put(ctx, datastore.NewKey(syncPrefix+pubID.String()), frozenAt.Bytes())
		if err != nil {
			log.Errorw("Unfreeze cannot set advertisement as last processed", "err", err)
		}
	}
	return nil
}

func removeProcessedFrozen(ctx context.Context, dstore datastore.Datastore) error {
	q := query.Query{
		Prefix:   adProcessedFrozenPrefix,
		KeysOnly: true,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return err
	}

	ents, err := results.Rest()
	if err != nil {
		return err
	}

	for i := range ents {
		key := ents[i].Key
		err = dstore.Delete(ctx, datastore.NewKey(key))
		if err != nil {
			return err
		}
		err = dstore.Delete(ctx, datastore.NewKey(adProcessedPrefix+path.Base(key)))
		if err != nil {
			return err
		}
	}

	return nil
}

// Sync syncs advertisements, up to the latest advertisement, from a publisher.
// Sync returns the final CID that was synced by the call to Sync.
//
// Sync works by first fetching each advertisement from the specified peer
// starting at the most recent and traversing to the advertisement last seen by
// the indexer, or until the advertisement depth limit is reached. Then the
// entries in each advertisement are synced and the multihashes in each entry
// are indexed.
//
// The depth argument specifies the recursion depth limit to use during sync.
// Its value may less than -1 for no limit, 0 to use the indexer's configured
// value, or greater than 1 for an explicit limit.
//
// The resync argument specifies whether to stop the traversal at the latest
// known advertisement that is already synced. If set to true, the traversal
// will continue until either there are no more advertisements left or the
// recursion depth limit is reached.
//
// The reference to the latest synced advertisement returned by GetLatestSync
// is only updated if the given depth is zero and resync is set to false.
//
// The Context argument controls the lifetime of the sync. Canceling it cancels
// the sync and causes the multihash channel to close without any data.
func (ing *Ingester) Sync(ctx context.Context, peerInfo peer.AddrInfo, depth int, resync bool) (cid.Cid, error) {
	err := peerInfo.ID.Validate()
	if err != nil {
		return cid.Undef, errors.New("invalid provider id")
	}

	log := log.With("peer", peerInfo.ID, "addrs", peerInfo.Addrs, "depth", depth, "resync", resync)
	log.Info("Explicitly syncing the latest advertisement")

	// Ad chain traversal stops at the latest known head, unless resyncing.
	var opts []dagsync.SyncOption
	var latest cid.Cid
	if resync {
		// If this is a resync, then it is necessary to mark the ad as
		// unprocessed so that everything can be reingested from the start of
		// this sync. Create a scoped block-hook to do this.
		opts = append(opts, dagsync.WithAdsResync(true),
			dagsync.ScopedBlockHook(func(i peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
				if err := ing.markAdUnprocessed(c, true); err != nil {
					log.Errorw("Failed to mark ad as unprocessed", "err", err, "adCid", c)
				}
				// Call the general hook because scoped block hook overrides the
				// subscriber's general block hook.
				ing.generalDagsyncBlockHook(i, c, actions)
			}))
	} else {
		latest, err = ing.GetLatestSync(peerInfo.ID)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to get latest sync: %w", err)
		}
		// If explicitly syncing, then start from the last fully processed
		// advertisement and not the last seen advertisement.
		ing.sub.SetLatestSync(peerInfo.ID, latest)
	}

	if depth != 0 {
		opts = append(opts, dagsync.ScopedDepthLimit(int64(depth)))
	}

	syncDone, cancel := ing.onAdProcessed(peerInfo.ID)
	defer cancel()

	syncCtx := ctx
	if ing.syncTimeout != 0 {
		var syncCancel context.CancelFunc
		syncCtx, syncCancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer syncCancel()
	}

	// Start syncing. Notifications for the finished sync are sent
	// asynchronously. Sync with cid.Undef so that the latest head is queried
	// by dagsync via head-publisher.
	c, err := ing.sub.SyncAdChain(syncCtx, peerInfo, opts...)
	if err != nil {
		ing.reg.SetLastError(peerInfo.ID, err)
		return cid.Undef, fmt.Errorf("failed to sync: %w", err)
	}
	// Do not persist the latest sync here, because that is done after
	// processing the ad.

	// If latest head had already finished syncing, then do not wait for
	// syncDone since it will never happen.
	if !resync && latest == c {
		log.Infow("Latest advertisement already processed", "adCid", c)
		return c, nil
	}

	log.Debugw("Syncing advertisements up to latest", "adCid", c)
	for {
		select {
		case adProcessedEvent := <-syncDone:
			log.Debugw("Synced advertisement", "adCid", adProcessedEvent.adCid)
			if adProcessedEvent.err != nil {
				// If an error occurred then the adProcessedEvent.adCid will be
				// the cid that caused the error, and there will not be any
				// future adProcessedEvents. Therefore, check the headAdCid to
				// see if this was the sync that was started.
				if adProcessedEvent.headAdCid == c {
					return cid.Undef, adProcessedEvent.err
				}
			} else if adProcessedEvent.adCid == c {
				return c, nil
			}
		case <-ctx.Done():
			return cid.Undef, ctx.Err()
		case <-ing.closePendingSyncs:
			// When shutting down the ingester, calls to Sync may return "sync
			// closed" error, or this error may be returned first depending on
			// goroutine scheduling.
			return cid.Undef, errors.New("sync canceled: service closed")
		}
	}
}

// Announce sends an announce message to directly to dagsync, instead of
// through pubsub.
func (ing *Ingester) Announce(ctx context.Context, nextCid cid.Cid, pubAddrInfo peer.AddrInfo) error {
	log.Infow("Handling direct announce request", "peer", pubAddrInfo.ID, "cid", nextCid, "addrs", pubAddrInfo.Addrs)
	return ing.sub.Announce(ctx, nextCid, pubAddrInfo)
}

// markAdUnprocessed takes an advertisement CID and marks it as unprocessed.
// This lets the ad be re-ingested in case re-ingesting with different depths
// or processing even earlier ads and need to reprocess later ones so that the
// indexer re-ingests the later ones in the context of the earlier ads, and
// thus become consistent.
//
// During a sync, this should be called be in order from newest to oldest ad.
// This is so that if something fails to get marked as unprocessed, the
// constraint is maintained that if an ad is processed then all older ads are
// also processed.
//
// When forResync is true, index counts are not added to the existing index
// count.
func (ing *Ingester) markAdUnprocessed(adCid cid.Cid, forResync bool) error {
	data := []byte{0}
	if forResync {
		data = []byte{2}
	}
	return ing.ds.Put(context.Background(), datastore.NewKey(adProcessedPrefix+adCid.String()), data)
}

func (ing *Ingester) adAlreadyProcessed(adCid cid.Cid) (bool, bool) {
	v, err := ing.ds.Get(context.Background(), datastore.NewKey(adProcessedPrefix+adCid.String()))
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorw("Failed to read advertisement processed state from datastore", "err", err)
		}
		return false, false
	}
	processed := v[0] == byte(1)
	resync := v[0] == byte(2)
	return processed, resync
}

// MarkAdProcessed explicitly marks an advertisement as processed. This is used
// to avoid ingesting this and previous advertisements.
func (ing *Ingester) MarkAdProcessed(publisher peer.ID, adCid cid.Cid) error {
	return ing.markAdProcessed(publisher, adCid, false, false)
}

func (ing *Ingester) markAdProcessed(publisher peer.ID, adCid cid.Cid, frozen, mirrored bool) error {
	cidStr := adCid.String()
	ctx := context.Background()
	if frozen {
		err := ing.ds.Put(ctx, datastore.NewKey(adProcessedFrozenPrefix+cidStr), []byte{1})
		if err != nil {
			return err
		}
	}

	err := ing.ds.Put(ctx, datastore.NewKey(adProcessedPrefix+cidStr), []byte{1})
	if err != nil {
		return err
	}

	if !mirrored || frozen {
		// This ad is processed, so remove it from the datastore.
		err = ing.dsTmp.Delete(ctx, datastore.NewKey(cidStr))
		if err != nil {
			// Log the error, but do not return. Continue on to save the procesed ad.
			log.Errorw("Cannot remove advertisement from datastore", "err", err)
		}
	}

	return ing.ds.Put(ctx, datastore.NewKey(syncPrefix+publisher.String()), adCid.Bytes())
}

// distributeEvents reads a adProcessedEvent, sent by a peer handler, and
// copies the event to all channels in outEventsChans. This delivers the event
// to all onAdProcessed channel readers.
func (ing *Ingester) distributeEvents() {
	for event := range ing.inEvents {
		// Send update to all change notification channels.
		ing.outEventsMutex.Lock()
		pubEventsChans, ok := ing.outEventsChans[event.publisher]
		if ok {
			for _, ch := range pubEventsChans {
				ch <- event
			}
		}
		ing.outEventsMutex.Unlock()
	}
}

// onAdProcessed creates a channel that receives notification when an
// advertisement and all of its content entries have finished syncing.
//
// Doing a manual sync will not always cause a notification if the requested
// advertisement has previously been processed.
//
// Calling the returned cancel function removes the notification channel from
// the list of channels to be notified on changes, and closes the channel to
// allow any reading goroutines to stop waiting on the channel.
func (ing *Ingester) onAdProcessed(peerID peer.ID) (<-chan adProcessedEvent, context.CancelFunc) {
	// Use an infinite size channel so that it is impossible for it to fill
	// before being read. If this channel blocked and then caused
	// distributeEvents to block, that could prevent this channel from being
	// read, causing deadlock.
	cq := chanqueue.New[adProcessedEvent]()
	events := cq.In()
	cancel := func() {
		// Drain channel to prevent deadlock if blocked writes are preventing
		// the mutex from being unlocked.
		go func() {
			for range cq.Out() {
			}
		}()
		ing.outEventsMutex.Lock()
		defer ing.outEventsMutex.Unlock()
		pubEventsChans, ok := ing.outEventsChans[peerID]
		if !ok {
			log.Warnw("Advertisement processed notification already canceled", "peer", peerID)
			return
		}

		for i, ca := range pubEventsChans {
			if ca == events {
				if len(pubEventsChans) == 1 {
					if len(ing.outEventsChans) == 1 {
						ing.outEventsChans = nil
					} else {
						delete(ing.outEventsChans, peerID)
					}
				} else {
					pubEventsChans[i] = pubEventsChans[len(pubEventsChans)-1]
					pubEventsChans[len(pubEventsChans)-1] = nil
					ing.outEventsChans[peerID] = pubEventsChans[:len(pubEventsChans)-1]
				}
				close(events)
				break
			}
		}
	}

	ing.outEventsMutex.Lock()
	defer ing.outEventsMutex.Unlock()

	var pubEventsChans []chan<- adProcessedEvent
	if ing.outEventsChans == nil {
		ing.outEventsChans = make(map[peer.ID][]chan<- adProcessedEvent)
	} else {
		pubEventsChans = ing.outEventsChans[peerID]
	}
	ing.outEventsChans[peerID] = append(pubEventsChans, events)

	return cq.Out(), cancel
}

// metricsUpdate periodically updates metrics. This goroutine exits when
// canceling pending syncs, when Close is called.
func (ing *Ingester) metricsUpdater() {
	t := time.NewTimer(metricsUpdateInterval)
	var prevMhsFromMirror uint64
	for {
		select {
		case <-t.C:
			var usage float64
			usageStats, err := ing.reg.ValueStoreUsage()
			if err != nil {
				log.Errorw("Error getting disk usage", "err", err)
			} else if usageStats != nil {
				usage = usageStats.Percent
			}

			stats.Record(context.Background(),
				metrics.PercentUsage.M(usage))

			if ing.mirror.canRead() {
				mhsFromMirror := ing.MultihashesFromMirror()
				if mhsFromMirror != prevMhsFromMirror {
					log.Infow("Multihashes from mirror", "count", mhsFromMirror)
					prevMhsFromMirror = mhsFromMirror
				}
			}

			t.Reset(metricsUpdateInterval)
		case <-ing.closePendingSyncs:
			// If closing pending syncs, then close metrics updater as well.
			t.Stop()
			return
		}
	}
}

// removePublisher removes data for the identified publisher. This is done as
// part of removing a provider.
func (ing *Ingester) removePublisher(ctx context.Context, publisherID peer.ID) error {
	if publisherID.Validate() != nil {
		// Invalid publisher ID, registered provider never got a published ad.
		return nil
	}
	ing.sub.RemoveHandler(publisherID)
	err := ing.ds.Delete(ctx, datastore.NewKey(syncPrefix+publisherID.String()))
	if err != nil {
		return fmt.Errorf("could not remove latest sync for publisher %s: %w", publisherID, err)
	}
	return nil
}

func (ing *Ingester) autoSync() {
	var waitSyncs sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		// Cancel in-progress syncs started by autoSync, wait for them to
		// finish, and then signal that autoSync is done.
		cancel()
		waitSyncs.Wait()
		close(ing.autoSyncDone)
	}()

	var autoSyncMutex sync.Mutex
	autoSyncInProgress := map[peer.ID]struct{}{}

	for {
		var provInfo *registry.ProviderInfo
		var ok bool
		select {
		case provInfo, ok = <-ing.reg.SyncChan():
			if !ok {
				return
			}
		case <-ing.workersCtx.Done():
			return
		}

		if provInfo.Deleted() {
			if err := ing.removePublisher(ctx, provInfo.Publisher); err != nil {
				log.Errorw("Error removing provider", "err", err, "provider", provInfo.AddrInfo.ID)
			}
			// Do not remove provider info from core, because that requires
			// scanning the entire core valuestore. Instead, let the finder
			// delete provider contexts as deleted providers appear in find
			// results.
			continue
		}

		// If the provider has a stop cid, then that is the point to resume
		// handoff from. Mark that advertisement as processed so that future
		// indexing will proceed from that point forward.
		if stopCid := provInfo.StopCid(); stopCid != cid.Undef {
			err := ing.markAdProcessed(provInfo.Publisher, stopCid, false, false)
			if err != nil {
				// This error would cause the ingestion of everything from the
				// publisher from the handoff, and is a critical failure. It
				// also means that the datastore is failing to store data, so
				// likely nothing else is working.
				log.Errorw("Failed to mark ad from handoff as processed", "err", err)
				continue
			}
		}

		autoSyncMutex.Lock()
		_, already := autoSyncInProgress[provInfo.AddrInfo.ID]
		if already {
			log.Infow("Auto-sync already in progress", "provider", provInfo.AddrInfo.ID)
			autoSyncMutex.Unlock()
			continue
		}
		autoSyncInProgress[provInfo.AddrInfo.ID] = struct{}{}
		autoSyncMutex.Unlock()

		// Attempt to sync the provider at its last know publisher, in a
		// separate goroutine.
		waitSyncs.Add(1)
		go func(pubID peer.ID, pubAddr multiaddr.Multiaddr, provID peer.ID) {
			defer func() {
				autoSyncMutex.Lock()
				delete(autoSyncInProgress, provID)
				autoSyncMutex.Unlock()
				waitSyncs.Done()
			}()
			log := log.With("publisher", pubID, "addr", pubAddr)
			// Log provider ID if not the same as publisher ID.
			if provID != pubID {
				log = log.With("provider", provID)
			}
			log.Info("Auto-syncing the latest advertisement with publisher")

			// Sync from the last fully processed advertisement and not the
			// last seen advertisement.
			if latest, ok := ing.getLastKnownSync(pubID); ok {
				ing.sub.SetLatestSync(pubID, latest)
			}
			peerInfo := peer.AddrInfo{
				ID:    pubID,
				Addrs: []multiaddr.Multiaddr{pubAddr},
			}
			_, err := ing.sub.SyncAdChain(ctx, peerInfo)
			if err != nil {
				log.Errorw("Failed to auto-sync with publisher", "err", err)
				ing.reg.SetLastError(provID, fmt.Errorf("auto-sync failed: %s", err))
				return
			}
			ing.reg.Saw(provID, true)
		}(provInfo.Publisher, provInfo.PublisherAddr, provInfo.AddrInfo.ID)
	}
}

// GetLatestSync gets the latest CID synced for the peer. If no error is
// returned, then returned CID is never cid.Undef.
func (ing *Ingester) GetLatestSync(publisherID peer.ID) (cid.Cid, error) {
	b, err := ing.ds.Get(context.Background(), datastore.NewKey(syncPrefix+publisherID.String()))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	// Returns error if b is nil or empty.
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

// getLastKnownSync returns the CID of the last fully processed advertisement
// and a boolean indicating that a defined CID was successfully found.
func (ing *Ingester) getLastKnownSync(publisherID peer.ID) (cid.Cid, bool) {
	c, err := ing.GetLatestSync(publisherID)
	if err != nil {
		log.Errorw("Error getting last knonw sync", "err", err)
		return cid.Undef, false
	}
	return c, true
}

func (ing *Ingester) RunWorkers(n int) {
	ing.reg.SetMaxPoll(n / 2)
	for n > ing.workerPoolSize {
		// Start worker.
		ing.waitForWorkers.Add(1)
		go ing.ingestWorker(ing.workersCtx, ing.syncFinishedEvents, ing.nextWorkerNum)
		ing.workerPoolSize++
		ing.nextWorkerNum++
	}
	for n < ing.workerPoolSize {
		// Stop worker.
		ing.stopWorker <- struct{}{}
		ing.workerPoolSize--
	}
}

// ingestWorker processes events from dagsync.Subscriber, signaling that an
// advertisement chain has been synced. Provider work assignments are created
// from raw advertisement chains, and processed by ingestWorker. Work
// assignments are processed preferentially over new advertisement chains.
func (ing *Ingester) ingestWorker(ctx context.Context, syncFinishedEvents <-chan dagsync.SyncFinished, wkrNum int) {
	log := log.With("worker", wkrNum)
	log.Info("started ingest worker")
	defer ing.waitForWorkers.Done()

	for {
		log.Debug("ingest worker waiting for event")
		select {
		case event, ok := <-syncFinishedEvents:
			if !ok {
				log.Info("ingest worker exiting, sync finished events closed")
				return
			}

			pubID := event.PeerID

			if event.Err != nil {
				provID, ok := ing.reg.ProviderByPublisher(pubID)
				if ok {
					log.Debug("Setting last error for provider", "provider", provID, "publisher", pubID)
					ing.reg.SetLastError(provID, event.Err)
				}
				continue
			}

			log.Debugw("ingest worker processing raw ad chain", "publisher", pubID)

			if ing.putNextSyncFin(event) {
				// Ad chain is already being processed.
				log.Debugw("Ad chain already being processed, queued ad chain event", "publisher", pubID)
				continue
			}

			stats.Record(ctx, metrics.AdIngestActive.M(int64(ing.workersActive.Add(1))))

			for syncFin := ing.getNextSyncFin(pubID); syncFin != nil; syncFin = ing.getNextSyncFin(pubID) {
				ing.processRawAdChain(ctx, *syncFin, wkrNum)
			}

			stats.Record(ctx, metrics.AdIngestActive.M(int64(ing.workersActive.Add(-1))))
		case <-ctx.Done():
			log.Info("ingest worker canceled")
			return
		case <-ing.stopWorker:
			log.Info("ingest worker stopped")
			return
		}
	}
}

func (ing *Ingester) putNextSyncFin(event dagsync.SyncFinished) bool {
	pubID := event.PeerID

	ing.syncInProgressMu.Lock()
	defer ing.syncInProgressMu.Unlock()

	_, inProgress := ing.syncInProgress[pubID]
	ing.syncInProgress[pubID] = &event

	return inProgress
}

func (ing *Ingester) getNextSyncFin(pubID peer.ID) *dagsync.SyncFinished {
	ing.syncInProgressMu.Lock()
	defer ing.syncInProgressMu.Unlock()

	syncFin := ing.syncInProgress[pubID]
	if syncFin == nil {
		delete(ing.syncInProgress, pubID)
	} else {
		ing.syncInProgress[pubID] = nil
	}

	return syncFin
}

// processRawAdChain processes a raw advertisement chain from a publisher to
// create provider work assignments. When not workers are working on a work
// assignment for a provider, then workers are given the next work assignment
// for that provider.
func (ing *Ingester) processRawAdChain(ctx context.Context, syncFinished dagsync.SyncFinished, wkrNum int) {
	if syncFinished.Count == 0 {
		// Attempted sync, but already up to data. Nothing to do.
		return
	}

	publisher := syncFinished.PeerID
	log := log.With("publisher", publisher, "worker", wkrNum)
	log.Infow("Advertisement chain synced", "length", syncFinished.Count)

	var rmCount int64
	rmCtxID := make(map[string]struct{})

	// 1. Group the incoming CIDs by provider.
	//
	// Serializing on the provider prevents concurrent processing of ads for
	// the same provider from different chains. This allows the indexer to do
	// the following:
	//
	// - Detect and skip ads that have come from a different chain. Concurrent
	// processing may result in double processing. That would cause unnecessary
	// work and inaccuracy of metrics.
	//
	// - When an already-processed ad is seen, the indexer can abandon the
	// remainder of the chain being processed, instead of skipping ads for
	// specific providers on a mixed provider chain.
	adsGroupedByProvider := map[peer.ID][]adInfo{}
	provAddrs := map[peer.ID][]string{}
	var totalAds int64
	var nextAdCid cid.Cid

	for c := syncFinished.Cid; c != cid.Undef; c = nextAdCid {
		// Group the CIDs by the provider. Most of the time a publisher will
		// only publish Ads for one provider, but it's possible that an ad
		// chain can include multiple providers.

		processed, resync := ing.adAlreadyProcessed(c)
		if processed {
			// This ad has been processed so all earlier ads already have been
			// processed.
			log.Infow("Remainder of ad chain already processed", "cid", c)
			break
		}

		ad, err := ing.loadAd(c)
		if err != nil {
			stats.Record(context.Background(), metrics.AdLoadError.M(1))
			log.Errorw("Failed to load advertisement CID, skipping all remaining", "cid", c, "err", err)
			break
		}
		if ad.PreviousID != nil {
			nextAdCid = ad.PreviousID.(cidlink.Link).Cid
		} else {
			nextAdCid = cid.Undef
		}
		totalAds++

		providerID, err := peer.Decode(ad.Provider)
		if err != nil {
			log.Errorw("Failed to get provider from ad CID, skipping", "cid", c, "err", err)
			continue
		}
		// If this is the first ad for this provider, then save the provider
		// addresses.
		_, ok := provAddrs[providerID]
		if !ok && len(ad.Addresses) != 0 {
			provAddrs[providerID] = ad.Addresses
			log.Debugw("New provider seen in ad stack", "provider", providerID)
		}

		ai := adInfo{
			cid:    c,
			resync: resync,
		}

		ctxIdStr := string(ad.ContextID)
		if ad.IsRm {
			rmCtxID[ctxIdStr] = struct{}{}
			rmCount++
		} else if _, ok := rmCtxID[ctxIdStr]; ok {
			// This ad was deleted by a later remove.
			ai.skip = true
		}

		adsGroupedByProvider[providerID] = append(adsGroupedByProvider[providerID], ai)
		if totalAds%1000 == 0 {
			log.Debugf("Added %d ads to stack", totalAds)
		}
	}

	nonRmCount := totalAds - rmCount

	log.Debugw("Created ad stack", "providers", len(adsGroupedByProvider), "ads", totalAds, "rmCount", rmCount)

	stats.Record(ctx,
		metrics.RemoveAdCount.M(ing.totalRmAds.Add(rmCount)),
		metrics.NonRemoveAdCount.M(ing.totalNonRmAds.Add(nonRmCount)))

	// 2. For each provider put the ad stack to the worker msg channel. Each ad
	// stack contains ads for a single provider, from a single publisher.
	for providerID, adInfos := range adsGroupedByProvider {
		ing.providersBusyMu.Lock()
		if _, ok := ing.providersBusy[providerID]; ok {
			ing.providersBusyMu.Unlock()
			log.Errorw("Other worker already ingesting for same provider. Provider ad chain may by published at multiple locations.", "provider", providerID)
			return
		}
		ing.providersBusy[providerID] = struct{}{}
		ing.providersBusyMu.Unlock()

		ing.reg.Saw(providerID, false)
		ing.ingestWorkerLogic(ctx, providerID, publisher, provAddrs[providerID], adInfos, wkrNum)

		ing.providersBusyMu.Lock()
		delete(ing.providersBusy, providerID)
		ing.providersBusyMu.Unlock()
	}
}

func (ing *Ingester) ingestWorkerLogic(ctx context.Context, provider, publisher peer.ID, addresses []string, adInfos []adInfo, wkrNum int) {
	log := log.With("publisher", publisher, "worker", wkrNum)
	// Log provider ID if not the same as publisher ID.
	if provider != publisher {
		log = log.With("provider", provider)
	}

	headProvider := peer.AddrInfo{
		ID:    provider,
		Addrs: stringsToMultiaddrs(addresses),
	}
	headAdCid := adInfos[0].cid

	if ing.mirror.canWrite() && !adInfos[0].resync {
		_, err := ing.mirror.writeHead(ctx, headAdCid, provider)
		if err != nil {
			log.Errorw("Cannot write publisher head", "err", err)
		}
	}

	frozen := ing.reg.Frozen()
	skip500EntsErr := ing.skip500EntsErr.Load()

	total := len(adInfos)
	log.Infow("Running worker on ad stack", "headAdCid", headAdCid, "numAdsToProcess", total)
	var count int
	for i := len(adInfos) - 1; i >= 0; i-- {
		// Note that iteration proceeds backwards here. Earliest to newest.
		ai := adInfos[i]
		adInfos[i] = adInfo{} // Clear the adInfo to free memory.
		count++

		if ctx.Err() != nil {
			log.Infow("Ingest worker canceled while processing ads", "err", ctx.Err())
			ing.inEvents <- adProcessedEvent{
				publisher: publisher,
				headAdCid: headAdCid,
				adCid:     ai.cid,
				err:       ctx.Err(),
			}
			log.Debug("Sent ad processed event for canceled processing")
			return
		}

		processed, _ := ing.adAlreadyProcessed(ai.cid)
		if processed {
			log.Infow("Skipping advertisement that has already been processed",
				"adCid", ai.cid,
				"progress", fmt.Sprintf("%d of %d", count, total))
			continue
		}

		// If this ad is skipped because it gets deleted later in the chain,
		// then mark this ad as processed.
		if ai.skip {
			log.Infow("Skipping advertisement with deleted context",
				"adCid", ai.cid,
				"progress", fmt.Sprintf("%d of %d", count, total))

			if markErr := ing.markAdProcessed(publisher, ai.cid, frozen, false); markErr != nil {
				log.Errorw("Failed to mark ad as processed", "err", markErr)
			}
			// Do not write removed ads to mirror. They are not read during
			// indexing, and those deleted in the future are removed by GC.

			// Distribute the atProcessedEvent notices to waiting Sync calls.
			ing.inEvents <- adProcessedEvent{
				publisher: publisher,
				headAdCid: headAdCid,
				adCid:     ai.cid,
			}
			log.Debug("Sent ad processed event for skipped ad")
			continue
		}

		lag := total - count
		log.Infow("Processing advertisement",
			"adCid", ai.cid,
			"progress", fmt.Sprintf("%d of %d", count, total),
			"lag", lag)

		hasEnts, fromMirror, err := ing.ingestAd(ctx, publisher, ai.cid, ai.resync, frozen, lag, headProvider, wkrNum)
		if err != nil {
			var adIngestErr adIngestError
			if errors.As(err, &adIngestErr) {
				switch adIngestErr.state {
				case adIngestDecodingErr, adIngestMalformedErr, adIngestEntryChunkErr, adIngestContentNotFound:
					// These error cases are permanent. If retried later the same
					// error will happen. So log and drop this error.
					log.Errorw("Skipping ad because of a permanent error", "adCid", ai.cid, "err", err, "errKind", adIngestErr.state)
					stats.Record(context.Background(), metrics.AdIngestSkippedCount.M(1))
					err = nil
				case adIngestSyncEntriesErr:
					if skip500EntsErr && strings.Contains(err.Error(), "failed to sync first entry") && strings.Contains(err.Error(), ": 500") {
						log.Errorw("Skipping ad because of a permanent 500 error", "adCid", ai.cid, "err", err, "errKind", adIngestErr.state)
						stats.Record(context.Background(), metrics.AdIngestSkippedCount.M(1))
						err = nil
					}
				}
				stats.RecordWithOptions(context.Background(),
					stats.WithMeasurements(metrics.AdIngestErrorCount.M(1)),
					stats.WithTags(tag.Insert(metrics.ErrKind, string(adIngestErr.state))))
			} else {
				stats.RecordWithOptions(context.Background(),
					stats.WithMeasurements(metrics.AdIngestErrorCount.M(1)),
					stats.WithTags(tag.Insert(metrics.ErrKind, "other error")))
			}

			// If err still not nil, then this is a non-permanent type of error.
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					errText := err.Error()
					if errors.Is(err, errInternal) {
						errText = errInternal.Error()
					}
					ing.reg.SetLastError(provider, fmt.Errorf("error while ingesting ad %s: %s", ai.cid, errText))
				}
				log.Errorw("Error while ingesting ad. Bailing early, not ingesting later ads.", "adCid", ai.cid, "err", err, "adsLeftToProcess", i+1)
				// Tell anyone waiting that the sync finished for this head because
				// of error.  TODO(mm) would be better to propagate the error.
				ing.inEvents <- adProcessedEvent{
					publisher: publisher,
					headAdCid: headAdCid,
					adCid:     ai.cid,
					err:       err,
				}
				log.Debug("Sent ad processed event with error")
				return
			}
		} else {
			// No error at all, this ad was processed successfully.
			stats.Record(context.Background(), metrics.AdIngestSuccessCount.M(1))
		}

		ing.reg.SetLastError(provider, nil)

		putMirror := hasEnts && ing.mirror.canWrite()
		if markErr := ing.markAdProcessed(publisher, ai.cid, frozen, putMirror); markErr != nil {
			log.Errorw("Failed to mark ad as processed", "err", markErr)
		}

		if putMirror {
			if fromMirror && ing.mirror.readWriteSame() {
				log.Debug("Removing temporary ad data")
				// If ad data retrieved from same mirror that is being written
				// to, then only clean up the data from local datastore, but do
				// not rewrite it to the mirror.
				err = ing.mirror.cleanupAdData(ctx, ai.cid, false)
				if err != nil {
					log.Errorw("Cannot remove advertisement data from datastore", "err", err)
				}
			} else {
				log.Debug("Writing ad to CAR mirror")
				// If resyncing and not overwriting, then do not overwrite the
				// destination file if it already exists.
				preventOverwrite := ai.resync && !ing.overwriteMirrorOnResync

				carInfo, err := ing.mirror.write(ctx, ai.cid, false, preventOverwrite)
				if err != nil {
					if !errors.Is(err, fs.ErrExist) {
						// Log the error, but do not return. Continue on to save the procesed ad.
						log.Errorw("Cannot write advertisement to CAR file", "err", err)
					}
				} else {
					log.Infow("Wrote CAR for advertisement", "path", carInfo.Path, "size", carInfo.Size)
				}
			}
		}

		log.Debug("Done processing ad")

		// Distribute the atProcessedEvent notices to waiting Sync calls.
		ing.inEvents <- adProcessedEvent{
			publisher: publisher,
			headAdCid: headAdCid,
			adCid:     ai.cid,
		}
		log.Debug("Sent ad processed event")
	}
}
