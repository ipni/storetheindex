package ingest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/channelqueue"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	indexer "github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/counter"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/peerutil"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"golang.org/x/time/rate"
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

// pendingAnnounce captures an announcement received from a provider that await processing.
type pendingAnnounce struct {
	addrInfo peer.AddrInfo
	nextCid  cid.Cid
}

type adInfo struct {
	cid    cid.Cid
	resync bool
	skip   bool
}

type workerAssignment struct {
	// none represents a nil assignment. Used because a nil in atomic.Value
	// cannot be stored.
	none      bool
	addresses []string
	adInfos   []adInfo
	publisher peer.ID
	provider  peer.ID
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
	lsys    ipld.LinkSystem
	indexer indexer.Interface

	batchSize uint32
	closeOnce sync.Once

	sub         *dagsync.Subscriber
	syncTimeout time.Duration

	entriesSel datamodel.Node
	reg        *registry.Registry

	// inEvents is used to send an adProcessedEvent to the distributeEvents
	// goroutine, when an advertisement in marked complete or having an error.
	inEvents chan adProcessedEvent

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of an adProcessedEvent to an onAdProcessed reader.
	outEventsChans map[peer.ID][]chan<- adProcessedEvent
	outEventsMutex sync.Mutex

	autoSyncDone      chan struct{}
	closePendingSyncs chan struct{}

	// A map of providers currently being processed. A worker holds the lock of
	// a provider while ingesting ads for that provider.
	providersBeingProcessed   map[peer.ID]chan struct{}
	providersBeingProcessedMu sync.Mutex
	providerWorkAssignment    map[peer.ID]*atomic.Value

	// Used to stop watching for sync finished events from dagsync.
	cancelOnSyncFinished context.CancelFunc

	// Channels that workers read from.
	syncFinishedEvents <-chan dagsync.SyncFinished
	workReady          chan peer.ID

	// Context and cancel function used to cancel all workers.
	cancelWorkers context.CancelFunc
	workersCtx    context.Context

	// Worker pool resizing.
	stopWorker     chan struct{}
	waitForWorkers sync.WaitGroup
	workerPoolSize int

	// RateLimiting
	rateApply peerutil.Policy
	rateBurst int

	// providersPendingAnnounce maps the provider ID to the latest announcement
	// received from the provider that is waiting to be processed.
	providersPendingAnnounce sync.Map

	rateLimit rate.Limit
	rateMutex sync.Mutex

	// Multihash minimum length
	minKeyLen int

	mirror        adMirror
	mhsFromMirror uint64

	// Metrics
	activeWorkers int32
	queuedWorkers int32
	backlogs      map[peer.ID]int32
	totalBacklog  int32
	indexCounts   *counter.IndexCounts
}

// NewIngester creates a new Ingester that uses a dagsync Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr indexer.Interface, reg *registry.Registry, ds datastore.Batching, options ...Option) (*Ingester, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	if cfg.IngestWorkerCount == 0 {
		return nil, errors.New("ingester worker count must be > 0")
	}

	ing := &Ingester{
		host:        h,
		ds:          ds,
		lsys:        mkLinkSystem(ds, reg),
		indexer:     idxr,
		batchSize:   uint32(cfg.StoreBatchSize),
		syncTimeout: time.Duration(cfg.SyncTimeout),
		entriesSel:  Selectors.EntriesWithLimit(recursionLimit(cfg.EntriesDepthLimit)),
		reg:         reg,
		inEvents:    make(chan adProcessedEvent, 1),

		autoSyncDone:      make(chan struct{}),
		closePendingSyncs: make(chan struct{}),

		providersBeingProcessed: make(map[peer.ID]chan struct{}),
		providerWorkAssignment:  make(map[peer.ID]*atomic.Value),
		stopWorker:              make(chan struct{}),

		workReady: make(chan peer.ID, 1),

		minKeyLen: cfg.MinimumKeyLength,

		indexCounts: opts.idxCounts,
		backlogs:    make(map[peer.ID]int32),
	}

	ing.workersCtx, ing.cancelWorkers = context.WithCancel(context.Background())

	ing.mirror, err = newMirror(cfg.AdvertisementMirror, ds)
	if err != nil {
		return nil, err
	}

	ing.rateApply, ing.rateBurst, ing.rateLimit, err = configRateLimit(cfg.RateLimit)
	if err != nil {
		log.Error(err.Error())
	}

	// Instantiate retryable HTTP client used by dagsync httpsync.
	rclient := &retryablehttp.Client{
		HTTPClient: &http.Client{
			Timeout: time.Duration(cfg.HttpSyncTimeout),
		},
		RetryWaitMin: time.Duration(cfg.HttpSyncRetryWaitMin),
		RetryWaitMax: time.Duration(cfg.HttpSyncRetryWaitMax),
		RetryMax:     cfg.HttpSyncRetryMax,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      retryablehttp.DefaultBackoff,
	}

	// Create and start pubsub subscriber. This also registers the storage hook
	// to index data as it is received.
	sub, err := dagsync.NewSubscriber(h, ds, ing.lsys, cfg.PubSubTopic, Selectors.AdSequence,
		dagsync.AllowPeer(reg.Allowed),
		dagsync.FilterIPs(reg.FilterIPsEnabled()),
		dagsync.SyncRecursionLimit(recursionLimit(cfg.AdvertisementDepthLimit)),
		dagsync.UseLatestSyncHandler(&syncHandler{ing}),
		dagsync.RateLimiter(ing.getRateLimiter),
		dagsync.SegmentDepthLimit(int64(cfg.SyncSegmentDepthLimit)),
		dagsync.HttpClient(rclient.StandardClient()),
		dagsync.BlockHook(ing.generalDagsyncBlockHook),
		dagsync.ResendAnnounce(cfg.ResendDirectAnnounce),
	)

	if err != nil {
		log.Errorw("Failed to start pubsub subscriber", "err", err)
		return nil, errors.New("ingester subscriber failed")
	}
	ing.sub = sub

	ing.syncFinishedEvents, ing.cancelOnSyncFinished = ing.sub.OnSyncFinished()

	ing.RunWorkers(cfg.IngestWorkerCount)

	// Start distributor to send SyncFinished messages to interested parties.
	go ing.distributeEvents()

	go ing.metricsUpdater()

	go ing.autoSync()

	log.Debugf("Ingester started and all hooks and linksystem registered")

	return ing, nil
}

func (ing *Ingester) MultihashesFromMirror() uint64 {
	return atomic.LoadUint64(&ing.mhsFromMirror)
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

func (ing *Ingester) getRateLimiter(publisher peer.ID) *rate.Limiter {
	ing.rateMutex.Lock()
	defer ing.rateMutex.Unlock()

	// If rateLimiting disabled or publisher is not rate-limited, then return
	// infinite rate limiter.
	if ing.rateLimit == 0 || !ing.rateApply.Eval(publisher) {
		return rate.NewLimiter(rate.Inf, 0)
	}
	// Return rate limiter with rate setting from config.
	return rate.NewLimiter(ing.rateLimit, ing.rateBurst)
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
// The selector used to sync the advertisement is controlled by the following
// parameters: depth, and resync.
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
// is only updated if the given depth is zero and resync is set to
// false. Otherwise, a custom selector with the given depth limit and stop link
// is constructed and used for traversal. See dagsync.Subscriber.Sync.
//
// The Context argument controls the lifetime of the sync. Canceling it cancels
// the sync and causes the multihash channel to close without any data.
func (ing *Ingester) Sync(ctx context.Context, peerInfo peer.AddrInfo, depth int, resync bool) (cid.Cid, error) {
	err := peerInfo.ID.Validate()
	if err != nil {
		return cid.Undef, errors.New("invalid provider id")
	}

	log := log.With("publisher", peerInfo.ID, "addrs", peerInfo.Addrs, "depth", depth, "resync", resync)
	log.Info("Explicitly syncing the latest advertisement from peer")

	var sel ipld.Node
	// If depth is non-zero or traversal should not stop at the latest synced,
	// then construct a selector to behave accordingly.
	if depth != 0 || resync {
		sel, err = ing.makeLimitedDepthSelector(peerInfo.ID, depth, resync)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to construct selector for explicit sync: %w", err)
		}
	}

	latest, err := ing.GetLatestSync(peerInfo.ID)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get latest sync: %w", err)
	}

	// Start syncing. Notifications for the finished sync are sent
	// asynchronously. Sync with cid.Undef so that the latest head is queried
	// by dagsync via head-publisher.
	//
	// Note that if the selector is nil the default selector is used where
	// traversal stops at the latest known head.
	//
	// Reference to the latest synced CID is only updated if the given selector
	// is nil.
	opts := []dagsync.SyncOption{
		dagsync.AlwaysUpdateLatest(),
	}
	if resync {
		// If this is a resync, then it is necessary to mark the ad as
		// unprocessed so that everything can be reingested from the start of
		// this sync. Create a scoped block-hook to do this.
		opts = append(opts, dagsync.ScopedBlockHook(func(i peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
			err := ing.markAdUnprocessed(c, true)
			if err != nil {
				log.Errorw("Failed to mark ad as unprocessed", "err", err, "adCid", c)
			}
			// Call the general hook because scoped block hook overrides the
			// subscriber's general block hook.
			ing.generalDagsyncBlockHook(i, c, actions)
		}))
	}

	syncDone, cancel := ing.onAdProcessed(peerInfo.ID)
	defer cancel()

	syncCtx := ctx
	if ing.syncTimeout != 0 {
		var syncCancel context.CancelFunc
		syncCtx, syncCancel = context.WithTimeout(ctx, ing.syncTimeout)
		defer syncCancel()
	}

	c, err := ing.sub.Sync(syncCtx, peerInfo, cid.Undef, sel, opts...)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to sync: %w", err)
	}
	// Do not persist the latest sync here, because that is done after
	// processing the ad.

	// If latest head had already finished syncing, then do not wait for
	// syncDone since it will never happen.
	if latest == c && !resync {
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

// Announce send an announce message to directly to dagsync, instead of through
// pubsub.
func (ing *Ingester) Announce(ctx context.Context, nextCid cid.Cid, pubAddrInfo peer.AddrInfo) error {
	publisher := pubAddrInfo.ID
	log := log.With("publisher", publisher, "cid", nextCid, "addrs", pubAddrInfo.Addrs)

	// If the publisher is not the same as the provider, then this will not
	// wait for the provider to be done processing the ad chain it is working
	// on.
	ing.providersBeingProcessedMu.Lock()
	pc, ok := ing.providersBeingProcessed[publisher]
	ing.providersBeingProcessedMu.Unlock()
	if !ok {
		return ing.sub.Announce(ctx, nextCid, publisher, pubAddrInfo.Addrs)
	}

	// The publisher in the announce message has the same ID as a known
	// provider, so defer handling the announce if that provider is busy.
	select {
	case pc <- struct{}{}:
		log.Info("Handling direct announce request")
		err := ing.sub.Announce(ctx, nextCid, publisher, pubAddrInfo.Addrs)
		<-pc
		return err
	case <-ctx.Done():
		return ctx.Err()
	default:
		ing.providersPendingAnnounce.Store(publisher, pendingAnnounce{
			addrInfo: pubAddrInfo,
			nextCid:  nextCid,
		})
		log.Info("Deferred handling direct announce request")
		return nil
	}
}

func (ing *Ingester) makeLimitedDepthSelector(peerID peer.ID, depth int, resync bool) (ipld.Node, error) {
	// Consider the value of < 1 as no-limit.
	rLimit := recursionLimit(depth)
	log := log.With("depth", depth)

	var stopAt ipld.Link
	if !resync {
		latest, err := ing.GetLatestSync(peerID)
		if err != nil {
			return nil, err
		}

		if latest != cid.Undef {
			stopAt = cidlink.Link{Cid: latest}
		}
	}
	// The stop link may be nil, in which case it is treated as no stop link.
	// Log it regardless for debugging purposes.
	log.Debugw("Custom selector constructed for explicit sync", "stopAt", stopAt)
	return dagsync.ExploreRecursiveWithStopNode(rLimit, Selectors.AdSequence, stopAt), nil
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

func (ing *Ingester) markAdProcessed(publisher peer.ID, adCid cid.Cid, frozen, keep bool) error {
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

	if !keep || frozen {
		// This ad is processed, so remove it from the datastore.
		err = ing.ds.Delete(ctx, datastore.NewKey(cidStr))
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
	cq := channelqueue.New[adProcessedEvent](-1)
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
			log.Warnw("Advertisement processed notification already cancelled", "peerID", peerID)
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
			// Update value store size metric after sync.
			size, err := ing.indexer.Size()
			if err != nil {
				log.Errorw("Error getting indexer value store size", "err", err)
			}
			var usage float64
			usageStats, err := ing.reg.ValueStoreUsage()
			if err != nil {
				log.Errorw("Error getting disk usage", "err", err)
			} else {
				usage = usageStats.Percent
			}

			if ing.indexCounts != nil {
				indexCount, err := ing.indexCounts.Total()
				if err != nil {
					log.Errorw("Error getting index counts", "err", err)
				}
				stats.Record(context.Background(),
					coremetrics.StoreSize.M(size),
					metrics.IndexCount.M(int64(indexCount)),
					metrics.PercentUsage.M(usage))
			} else {
				stats.Record(context.Background(),
					coremetrics.StoreSize.M(size),
					metrics.PercentUsage.M(usage))
			}

			if ing.mirror.canRead() {
				mhsFromMirror := ing.MultihashesFromMirror()
				if mhsFromMirror != prevMhsFromMirror {
					log.Infof("Multihashes from mirror: %d", mhsFromMirror)
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
			if ing.indexCounts != nil {
				ing.indexCounts.RemoveProvider(provInfo.AddrInfo.ID)
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

			peerInfo := peer.AddrInfo{
				ID:    pubID,
				Addrs: []multiaddr.Multiaddr{pubAddr},
			}
			_, err := ing.sub.Sync(ctx, peerInfo, cid.Undef, nil)
			if err != nil {
				log.Errorw("Failed to auto-sync with publisher", "err", err)
				return
			}
			ing.reg.Saw(provID)
		}(provInfo.Publisher, provInfo.PublisherAddr, provInfo.AddrInfo.ID)
	}
}

// GetLatestSync gets the latest CID synced for the peer.
func (ing *Ingester) GetLatestSync(publisherID peer.ID) (cid.Cid, error) {
	b, err := ing.ds.Get(context.Background(), datastore.NewKey(syncPrefix+publisherID.String()))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

// BatchSize returns the current batch size.
func (ing *Ingester) BatchSize() int {
	return int(atomic.LoadUint32(&ing.batchSize))
}

// SetBatchSize sets the batch size.
func (ing *Ingester) SetBatchSize(batchSize int) {
	atomic.StoreUint32(&ing.batchSize, uint32(batchSize))
}

// SetRateLimit sets the rate limit.
func (ing *Ingester) SetRateLimit(cfgRateLimit config.RateLimit) error {
	apply, burst, limit, err := configRateLimit(cfgRateLimit)
	if err != nil {
		return err
	}

	ing.rateMutex.Lock()

	ing.rateApply = apply
	ing.rateBurst = burst
	ing.rateLimit = limit

	ing.rateMutex.Unlock()
	return nil
}

func (ing *Ingester) RunWorkers(n int) {
	for n > ing.workerPoolSize {
		// Start worker.
		ing.waitForWorkers.Add(1)
		go ing.ingestWorker(ing.workersCtx, ing.syncFinishedEvents)
		ing.workerPoolSize++
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
func (ing *Ingester) ingestWorker(ctx context.Context, syncFinishedEvents <-chan dagsync.SyncFinished) {
	log.Debug("started ingest worker")
	defer ing.waitForWorkers.Done()

	for {
		// Wait for work only. Work assignments take priority over new
		// advertisement chains.
		select {
		case provider := <-ing.workReady:
			ing.handleWorkReady(ctx, provider)
		case <-ctx.Done():
			log.Info("ingest worker canceled")
			return
		case <-ing.stopWorker:
			log.Debug("stopped ingest worker")
			return
		default:
			// No work assignments, so also check for new advertisement chains.
			select {
			case provider := <-ing.workReady:
				ing.handleWorkReady(ctx, provider)
			case event, ok := <-syncFinishedEvents:
				if !ok {
					log.Info("ingest worker exiting, sync finished events closed")
					return
				}
				ing.processRawAdChain(ctx, event)
			case <-ctx.Done():
				log.Info("ingest worker canceled")
				return
			case <-ing.stopWorker:
				log.Debug("stopped ingest worker")
				return
			}
		}
	}
}

// processRawAdChain processes a raw advertisement chain from a publisher to
// create provider work assignments. When not workers are working on a work
// assignment for a provider, then workers are given the next work assignment
// for that provider.
func (ing *Ingester) processRawAdChain(ctx context.Context, syncFinishedEvent dagsync.SyncFinished) {
	if len(syncFinishedEvent.SyncedCids) == 0 {
		// Attempted sync, but already up to data. Nothing to do.
		return
	}
	publisher := syncFinishedEvent.PeerID
	log := log.With("publisher", publisher)
	log.Infow("Advertisement chain synced", "length", len(syncFinishedEvent.SyncedCids))

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
	for _, c := range syncFinishedEvent.SyncedCids {
		// Group the CIDs by the provider. Most of the time a publisher will
		// only publish Ads for one provider, but it's possible that an ad
		// chain can include multiple providers.

		processed, resync := ing.adAlreadyProcessed(c)
		if processed {
			// This ad has been processed so all earlier ads already have been
			// processed.
			break
		}

		ad, err := ing.loadAd(c)
		if err != nil {
			stats.Record(context.Background(), metrics.AdLoadError.M(1))
			log.Errorw("Failed to load advertisement CID, skipping", "cid", c, "err", err)
			continue
		}
		providerID, err := peer.Decode(ad.Provider)
		if err != nil {
			log.Errorf("Failed to get provider from ad CID: %s skipping", err)
			continue
		}
		// If this is the first ad for this provider, then save the provider
		// addresses.
		_, ok := provAddrs[providerID]
		if !ok {
			provAddrs[providerID] = ad.Addresses
		}

		ai := adInfo{
			cid:    c,
			resync: resync,
		}

		ctxIdStr := string(ad.ContextID)
		// This ad was deleted by a later remove.
		if _, ok := rmCtxID[ctxIdStr]; ok {
			ai.skip = true
		} else if ad.IsRm {
			rmCtxID[ctxIdStr] = struct{}{}
		}

		adsGroupedByProvider[providerID] = append(adsGroupedByProvider[providerID], ai)
	}

	// 2. For each provider put the ad stack to the worker msg channel. Each ad
	// stack contains ads for a single provider, from a single publisher.
	for providerID, adInfos := range adsGroupedByProvider {
		ing.providersBeingProcessedMu.Lock()
		if _, ok := ing.providersBeingProcessed[providerID]; !ok {
			ing.providersBeingProcessed[providerID] = make(chan struct{}, 1)
		}
		wa, ok := ing.providerWorkAssignment[providerID]
		if !ok {
			wa = &atomic.Value{}
			ing.providerWorkAssignment[providerID] = wa
		}
		ing.providersBeingProcessedMu.Unlock()

		oldAssignment := wa.Swap(workerAssignment{
			adInfos:   adInfos,
			addresses: provAddrs[providerID],
			publisher: publisher,
			provider:  providerID,
		})
		if oldAssignment == nil || oldAssignment.(workerAssignment).none {
			// No previous run scheduled a worker to handle this provider, so
			// schedule one.
			ing.reg.Saw(providerID)

			go func(provID peer.ID) {
				ing.providersBeingProcessedMu.Lock()
				provBusy := ing.providersBeingProcessed[provID]
				ing.backlogs[provID]++
				ing.providersBeingProcessedMu.Unlock()

				stats.Record(ctx,
					metrics.AdIngestBacklog.M(int64(atomic.AddInt32(&ing.totalBacklog, 1))),
					metrics.AdIngestQueued.M(int64(atomic.AddInt32(&ing.queuedWorkers, 1))))
				// Wait until the no workers are doing work for the provider
				// before notifying that another work assignment is available.
				select {
				case provBusy <- struct{}{}:
				case <-ctx.Done():
					return
				}
				select {
				case ing.workReady <- provID:
				case <-ctx.Done():
					return
				}
			}(providerID)
		}
		// If oldAssignment has adInfos, it is not necessary to merge the old
		// and new assignments because the new assignment will already have all
		// the adInfos that the old assignment does. If the old assignment was
		// not processed yet, then the sync that created the new assignment
		// would have traversed the same chain as the old. In other words, any
		// existing old assignment is always a subset of a new assignment.
	}
}

func (ing *Ingester) handleWorkReady(ctx context.Context, provider peer.ID) {
	ing.providersBeingProcessedMu.Lock()
	provBusy := ing.providersBeingProcessed[provider]
	n := ing.backlogs[provider]
	delete(ing.backlogs, provider)
	// Pull out the assignment for this provider, which was populated by processAdChain.
	wa := ing.providerWorkAssignment[provider]
	ing.providersBeingProcessedMu.Unlock()

	stats.Record(context.Background(),
		metrics.AdIngestBacklog.M(int64(atomic.AddInt32(&ing.totalBacklog, -n))),
		metrics.AdIngestQueued.M(int64(atomic.AddInt32(&ing.queuedWorkers, -1))),
		metrics.AdIngestActive.M(int64(atomic.AddInt32(&ing.activeWorkers, 1))))

	assignmentInterface := wa.Swap(workerAssignment{none: true})
	if assignmentInterface != nil {
		ing.ingestWorkerLogic(ctx, provider, assignmentInterface.(workerAssignment))
	}

	ing.handlePendingAnnounce(ctx, provider)

	// Signal that the worker is done with the provider.
	<-provBusy

	stats.Record(context.Background(), metrics.AdIngestActive.M(int64(atomic.AddInt32(&ing.activeWorkers, -1))))
}

func (ing *Ingester) ingestWorkerLogic(ctx context.Context, provider peer.ID, assignment workerAssignment) {
	if assignment.none {
		// Nothing to do.
		return
	}
	frozen := ing.reg.Frozen()

	log := log.With("publisher", assignment.publisher)
	// Log provider ID if not the same as publisher ID.
	if provider != assignment.publisher {
		log = log.With("provider", provider)
	}

	headProvider := peer.AddrInfo{
		ID:    provider,
		Addrs: stringsToMultiaddrs(assignment.addresses),
	}
	headAdCid := assignment.adInfos[0].cid

	total := len(assignment.adInfos)
	log.Infow("Running worker on ad stack", "headAdCid", headAdCid, "numAdsToProcess", total)
	var count int
	for i := len(assignment.adInfos) - 1; i >= 0; i-- {
		// Note that iteration proceeds backwards here. Earliest to newest.
		ai := assignment.adInfos[i]
		assignment.adInfos[i] = adInfo{} // Clear the adInfo to free memory.
		count++

		if ctx.Err() != nil {
			log.Infow("Ingest worker canceled while processing ads", "err", ctx.Err())
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: headAdCid,
				adCid:     ai.cid,
				err:       ctx.Err(),
			}
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

			keep := ing.mirror.canWrite()
			if markErr := ing.markAdProcessed(assignment.publisher, ai.cid, frozen, keep); markErr != nil {
				log.Errorw("Failed to mark ad as processed", "err", markErr)
			}
			if !frozen && keep {
				// Write the advertisement to a CAR file, but omit the entries.
				carInfo, err := ing.mirror.write(ctx, ai.cid, true)
				if err != nil {
					if !errors.Is(err, fs.ErrExist) {
						// Log the error, but do not return. Continue on to save the procesed ad.
						log.Errorw("Cannot write advertisement to CAR file", "err", err)
					}
					// else car file already exists
				} else {
					log.Infow("Wrote CAR for skipped advertisement", "path", carInfo.Path, "size", carInfo.Size)
					// TODO: Move this when iterating latest (head) to earliest (root).
					_, err = ing.mirror.writeHead(ctx, ai.cid, assignment.publisher)
					if err != nil {
						log.Errorw("Cannot write publisher head", "err", err)
					}
				}
			}

			// Distribute the atProcessedEvent notices to waiting Sync calls.
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: headAdCid,
				adCid:     ai.cid,
			}
			continue
		}

		lag := total - count
		log.Infow("Processing advertisement",
			"adCid", ai.cid,
			"progress", fmt.Sprintf("%d of %d", count, total),
			"lag", lag)

		err := ing.ingestAd(ctx, assignment.publisher, ai.cid, ai.resync, frozen, lag, headProvider)
		if err == nil {
			// No error at all, this ad was processed successfully.
			stats.Record(context.Background(), metrics.AdIngestSuccessCount.M(1))
		}

		var adIngestErr adIngestError
		if errors.As(err, &adIngestErr) {
			switch adIngestErr.state {
			case adIngestDecodingErr, adIngestMalformedErr, adIngestEntryChunkErr, adIngestContentNotFound:
				// These error cases are permanent. If retried later the same
				// error will happen. So log and drop this error.
				log.Errorw("Skipping ad because of a permanent error", "adCid", ai.cid, "err", err, "errKind", adIngestErr.state)
				stats.Record(context.Background(), metrics.AdIngestSkippedCount.M(1))
				err = nil
			}
			stats.RecordWithOptions(context.Background(),
				stats.WithMeasurements(metrics.AdIngestErrorCount.M(1)),
				stats.WithTags(tag.Insert(metrics.ErrKind, string(adIngestErr.state))))
		} else if err != nil {
			stats.RecordWithOptions(context.Background(),
				stats.WithMeasurements(metrics.AdIngestErrorCount.M(1)),
				stats.WithTags(tag.Insert(metrics.ErrKind, "other error")))
		}

		if err != nil {
			log.Errorw("Error while ingesting ad. Bailing early, not ingesting later ads.", "adCid", ai.cid, "err", err, "adsLeftToProcess", i+1)
			// Tell anyone waiting that the sync finished for this head because
			// of error.  TODO(mm) would be better to propagate the error.
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: headAdCid,
				adCid:     ai.cid,
				err:       err,
			}
			return
		}

		keep := ing.mirror.canWrite()
		if markErr := ing.markAdProcessed(assignment.publisher, ai.cid, frozen, keep); markErr != nil {
			log.Errorw("Failed to mark ad as processed", "err", markErr)
		}

		if !frozen && keep {
			carInfo, err := ing.mirror.write(ctx, ai.cid, false)
			if err != nil {
				if !errors.Is(err, fs.ErrExist) {
					// Log the error, but do not return. Continue on to save the procesed ad.
					log.Errorw("Cannot write advertisement to CAR file", "err", err)
				}
				// else car file already exists
			} else {
				log.Infow("Wrote CAR for advertisement", "path", carInfo.Path, "size", carInfo.Size)
				// TODO: Move this when iterating latest (head) to earliest (root).
				_, err = ing.mirror.writeHead(ctx, ai.cid, assignment.publisher)
				if err != nil {
					log.Errorw("Cannot write publisher head", "err", err)
				}
			}
		}

		// Distribute the atProcessedEvent notices to waiting Sync calls.
		ing.inEvents <- adProcessedEvent{
			publisher: assignment.publisher,
			headAdCid: headAdCid,
			adCid:     ai.cid,
		}
	}
}

func (ing *Ingester) handlePendingAnnounce(ctx context.Context, pubID peer.ID) {
	if ctx.Err() != nil {
		return
	}
	log := log.With("publisher", pubID)
	// Process pending announce request if any.
	// Note that the pending announce is deleted regardless of whether it was successfully
	// processed or not. Because, the cause of failure may be non-recoverable e.g. address
	// change and not removing it will block processing of future pending announces.
	v, found := ing.providersPendingAnnounce.LoadAndDelete(pubID)
	if !found {
		return
	}
	pa, ok := v.(pendingAnnounce)
	if !ok {
		log.Errorw("Cannot handle pending announce; unexpected type", "got", v)
		return
	}
	log = log.With("cid", pa.nextCid, "addrinfo", pa.addrInfo)
	err := ing.sub.Announce(ctx, pa.nextCid, pa.addrInfo.ID, pa.addrInfo.Addrs)
	if err != nil {
		log.Errorw("Failed to handle pending announce", "err", err)
		return
	}
	log.Info("Successfully handled pending announce")
}

// recursionLimit returns the recursion limit for the given depth.
func recursionLimit(depth int) selector.RecursionLimit {
	if depth < 1 {
		return selector.RecursionLimitNone()
	}
	return selector.RecursionLimitDepth(int64(depth))
}

func configRateLimit(cfgRateLimit config.RateLimit) (apply peerutil.Policy, burst int, limit rate.Limit, err error) {
	if cfgRateLimit.BlocksPerSecond == 0 {
		log.Info("rate limiting disabled")
		return
	}
	if cfgRateLimit.BlocksPerSecond < 0 {
		err = errors.New("BlocksPerSecond must be greater than or equal to 0")
		return
	}
	if cfgRateLimit.BurstSize < 0 {
		err = errors.New("BurstSize must be greater than or equal to 0")
		return
	}

	apply, err = peerutil.NewPolicyStrings(cfgRateLimit.Apply, cfgRateLimit.Except)
	if err != nil {
		err = fmt.Errorf("error setting rate limit for peers: %w", err)
		return
	}
	burst = cfgRateLimit.BurstSize
	limit = rate.Limit(cfgRateLimit.BlocksPerSecond)

	log.Info("rate limiting enabled")
	return
}
