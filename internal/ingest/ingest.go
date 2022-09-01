package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/peerutil"
	"github.com/gammazero/workerpool"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
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

type providerID peer.ID

// pendingAnnounce captures an announcement received from a provider that await processing.
type pendingAnnounce struct {
	addrInfo peer.AddrInfo
	nextCid  cid.Cid
}

type adInfo struct {
	cid cid.Cid
	ad  schema.Advertisement
}

type workerAssignment struct {
	// none represents a nil assignment. Used because a nil in atomic.Value
	// cannot be stored.
	none      bool
	adInfos   []adInfo
	publisher peer.ID
	provider  peer.ID
}

// Ingester is a type that uses go-legs for the ingestion protocol.
type Ingester struct {
	host    host.Host
	ds      datastore.Batching
	lsys    ipld.LinkSystem
	indexer indexer.Interface

	batchSize uint32
	closeOnce sync.Once
	sigUpdate chan struct{}

	sub         *legs.Subscriber
	syncTimeout time.Duration

	entriesSel datamodel.Node
	reg        *registry.Registry

	// inEvents is used to send a adProcessedEvent to the distributeEvents
	// goroutine, when an advertisement in marked complete or err'd.
	inEvents chan adProcessedEvent

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of an adProcessedEvent to an onAdProcessed reader.
	outEventsChans map[peer.ID][]chan adProcessedEvent
	outEventsMutex sync.Mutex

	waitForPendingSyncs sync.WaitGroup
	closePendingSyncs   chan struct{}
	cancelWorkers       context.CancelFunc
	workersCtx          context.Context

	cancelOnSyncFinished context.CancelFunc

	// A map of providers currently being processed. A worker holds the lock of
	// a provider while ingesting ads for that provider.
	providersBeingProcessed   map[peer.ID]chan struct{}
	providersBeingProcessedMu sync.Mutex
	providerAdChainStaging    map[peer.ID]*atomic.Value

	closeWorkers chan struct{}
	// toStaging receives sync finished events used to call to runIngestStep.
	toStaging <-chan legs.SyncFinished
	// toWorkers is used to ask the worker pool to start processing the ad
	// chain for a given provider.
	toWorkers      *Queue
	waitForWorkers sync.WaitGroup
	workerPoolSize int

	// RateLimiting
	rateApply peerutil.Policy
	rateBurst int

	// providersPendingAnnounce maps the provider ID to the latest announcement received from the
	// provider that is waiting to be processed.
	providersPendingAnnounce sync.Map

	rateLimit rate.Limit
	rateMutex sync.Mutex

	// Multihash minimum length
	minKeyLen int

	// entryWP is a worker pool for asynchronous calls to indexer.Put.
	entryWP *workerpool.WorkerPool
}

// NewIngester creates a new Ingester that uses a go-legs Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr indexer.Interface, reg *registry.Registry, ds datastore.Batching) (*Ingester, error) {

	ing := &Ingester{
		host:        h,
		ds:          ds,
		lsys:        mkLinkSystem(ds, reg),
		indexer:     idxr,
		batchSize:   uint32(cfg.StoreBatchSize),
		sigUpdate:   make(chan struct{}, 1),
		syncTimeout: time.Duration(cfg.SyncTimeout),
		entriesSel:  Selectors.EntriesWithLimit(recursionLimit(cfg.EntriesDepthLimit)),
		reg:         reg,
		inEvents:    make(chan adProcessedEvent, 1),

		closePendingSyncs: make(chan struct{}),

		providersBeingProcessed: make(map[peer.ID]chan struct{}),
		providerAdChainStaging:  make(map[peer.ID]*atomic.Value),
		toWorkers:               NewPriorityQueue(),
		closeWorkers:            make(chan struct{}),

		minKeyLen: cfg.MinimumKeyLength,
	}

	var err error
	ing.rateApply, ing.rateBurst, ing.rateLimit, err = configRateLimit(cfg.RateLimit)
	if err != nil {
		log.Error(err.Error())
	}

	// Instantiate retryable HTTP client used by legs httpsync.
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
	sub, err := legs.NewSubscriber(h, ds, ing.lsys, cfg.PubSubTopic, Selectors.AdSequence,
		legs.AllowPeer(reg.Allowed),
		legs.FilterIPs(reg.FilterIPsEnabled()),
		legs.SyncRecursionLimit(recursionLimit(cfg.AdvertisementDepthLimit)),
		legs.UseLatestSyncHandler(&syncHandler{ing}),
		legs.RateLimiter(ing.getRateLimiter),
		legs.SegmentDepthLimit(int64(cfg.SyncSegmentDepthLimit)),
		legs.HttpClient(rclient.StandardClient()),
		legs.BlockHook(ing.generalLegsBlockHook),
		legs.ResendAnnounce(cfg.ResendDirectAnnounce),
	)

	if err != nil {
		log.Errorw("Failed to start pubsub subscriber", "err", err)
		return nil, errors.New("ingester subscriber failed")
	}
	ing.sub = sub

	ing.toStaging, ing.cancelOnSyncFinished = ing.sub.OnSyncFinished()

	if cfg.IngestWorkerCount == 0 {
		return nil, errors.New("ingester worker count must be > 0")
	}

	if cfg.EntryPutConcurrency > 1 {
		ing.entryWP = workerpool.New(cfg.EntryPutConcurrency)
	}

	ing.workersCtx, ing.cancelWorkers = context.WithCancel(context.Background())
	ing.RunWorkers(cfg.IngestWorkerCount)
	go ing.runIngesterLoop()

	// Start distributor to send SyncFinished messages to interested parties.
	go ing.distributeEvents()

	go ing.metricsUpdater()

	go ing.autoSync()

	log.Debugf("Ingester started and all hooks and linksystem registered")

	return ing, nil
}

func (ing *Ingester) generalLegsBlockHook(_ peer.ID, c cid.Cid, actions legs.SegmentSyncActions) {
	// The only kind of block we should get by loading CIDs here should be Advertisement.
	// Because:
	//  - the default subscription selector only selects advertisements.
	//  - explicit Ingester.Sync only selects advertisement.
	//  - entries are synced with an explicit selector separate from advertisement syncs and
	//    should use legs.ScopedBlockHook to override this hook and decode chunks
	//    instead.
	//
	// Therefore, we only attempt to load advertisements here and signal failure if the
	// load fails.
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

func (ing *Ingester) Close() error {
	// Tell workers to stop ingestion in progress.
	ing.cancelWorkers()

	// Close leg transport.
	err := ing.sub.Close()
	log.Info("legs subscriber stopped")

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
		ing.cancelOnSyncFinished()
		close(ing.closeWorkers)
		ing.waitForWorkers.Wait()
		ing.toWorkers.Close()
		log.Info("Workers stopped")
		close(ing.closePendingSyncs)
		ing.waitForPendingSyncs.Wait()
		log.Info("Pending sync processing stopped")

		if ing.entryWP != nil {
			ing.entryWP.Stop()
			log.Info("Entry workers stopped")
		}

		// Stop the distribution goroutine.
		close(ing.inEvents)

		close(ing.sigUpdate)

		log.Info("Ingester stopped")
	})

	return err
}

// Sync syncs advertisements, up to the the latest advertisement, from a
// publisher. A channel is returned that gives the caller the option to wait
// for Sync to complete. The channel returns the final CID that was synced by
// the call to Sync.
//
// Sync works by first fetching each advertisement from the specidief peer
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
// is constructed and used for traversal. See legs.Subscriber.Sync.
//
// The Context argument controls the lifetime of the sync. Canceling it cancels
// the sync and causes the multihash channel to close without any data.
func (ing *Ingester) Sync(ctx context.Context, peerID peer.ID, peerAddr multiaddr.Multiaddr, depth int, resync bool) (<-chan cid.Cid, error) {
	if err := peerID.Validate(); err != nil {
		return nil, err
	}

	out := make(chan cid.Cid, 1)

	ing.waitForPendingSyncs.Add(1)
	go func() {
		defer ing.waitForPendingSyncs.Done()
		defer close(out)

		log := log.With("provider", peerID, "peerAddr", peerAddr, "depth", depth, "resync", resync)
		log.Info("Explicitly syncing the latest advertisement from peer")

		var sel ipld.Node
		// If depth is non-zero or traversal should not stop at the latest
		// synced, then construct a selector to behave accordingly.
		if depth != 0 || resync {
			var err error
			sel, err = ing.makeLimitedDepthSelector(peerID, depth, resync)
			if err != nil {
				log.Errorw("Failed to construct selector for explicit sync", "err", err)
				return
			}
		}

		syncDone, cancel := ing.onAdProcessed(peerID)
		defer cancel()

		latest, err := ing.GetLatestSync(peerID)
		if err != nil {
			log.Errorw("Failed to get latest sync", "err", err)
			return
		}

		// Start syncing. Notifications for the finished sync are sent
		// asynchronously. Sync with cid.Undef so that the latest head is
		// queried by go-legs via head-publisher.
		//
		// Note that if the selector is nil the default selector is used where
		// traversal stops at the latest known head.
		//
		// Reference to the latest synced CID is only updated if the given
		// selector is nil.
		opts := []legs.SyncOption{
			legs.AlwaysUpdateLatest(),
		}
		if resync {
			// If this is a resync, then it is necessary to mark the ad as
			// unprocessed so that everything can be reingested from the start
			// of this sync. Create a scoped block-hook to do this.
			opts = append(opts, legs.ScopedBlockHook(func(i peer.ID, c cid.Cid, actions legs.SegmentSyncActions) {
				err := ing.markAdUnprocessed(c)
				if err != nil {
					log.Errorw("Failed to mark ad as unprocessed", "err", err, "adCid", c)
				}
				// Call the general hook because scoped block hook overrides the subscriber's
				// general block hook.
				ing.generalLegsBlockHook(i, c, actions)
			}))
		}
		c, err := ing.sub.Sync(ctx, peerID, cid.Undef, sel, peerAddr, opts...)
		if err != nil {
			log.Errorw("Failed to sync with provider", "err", err)
			return
		}
		// Do not persist the latest sync here, because that is done after
		// processing the ad.

		// If latest head had already finished syncing, then do not wait for
		// syncDone since it will never happen.
		if latest == c && !resync {
			log.Infow("Latest advertisement already processed", "adCid", c)
			out <- c
			return
		}

		log.Debugw("Syncing advertisements up to latest", "adCid", c)
		for {
			select {
			case adProcessedEvent := <-syncDone:
				log.Debugw("Synced advertisement", "adCid", adProcessedEvent.adCid)
				if adProcessedEvent.adCid == c || adProcessedEvent.err != nil && adProcessedEvent.headAdCid == c {
					// If an error occurred then the adProcessedEvent.adCid
					// will be the cid that caused the error, and there will
					// not be any future adProcessedEvents. Therefore check the
					// headAdCid to see if this was the sync that was started.
					out <- c
					ing.signalMetricsUpdate()
					return
				}
			case <-ctx.Done():
				log.Warnw("Sync cancelled", "err", ctx.Err())
				return
			case <-ing.closePendingSyncs:
				log.Warnw("Sync cancelled because of close")
				return
			}
		}
	}()
	return out, nil
}

// Announce send an announce message to directly to go-legs, instead of through
// pubsub.
func (ing *Ingester) Announce(ctx context.Context, nextCid cid.Cid, addrInfo peer.AddrInfo) error {
	provider := addrInfo.ID
	log := log.With("provider", provider, "cid", nextCid, "addrs", addrInfo.Addrs)

	ing.providersBeingProcessedMu.Lock()
	pc, ok := ing.providersBeingProcessed[provider]
	if !ok {
		pc = make(chan struct{}, 1)
		ing.providersBeingProcessed[provider] = pc
	}
	ing.providersBeingProcessedMu.Unlock()

	select {
	case pc <- struct{}{}:
		log.Info("Handling direct announce request")
		err := ing.sub.Announce(ctx, nextCid, provider, addrInfo.Addrs)
		<-pc
		return err
	case <-ctx.Done():
		return ctx.Err()
	default:
		ing.providersPendingAnnounce.Store(provider, pendingAnnounce{
			addrInfo: addrInfo,
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
	return legs.ExploreRecursiveWithStopNode(rLimit, Selectors.AdSequence, stopAt), nil
}

// markAdUnprocessed takes an advertisement CID and marks it as
// unprocessed. This lets the ad be re-ingested in case we re-ingesting with
// different depths or are processing even earlier ads and need to reprocess
// later ones so that the indexer re-ingests the later ones in the context of
// the earlier ads, and thus become consistent.
//
// During a sync, this should be called be in order from newest to oldest
// ad. This is so that if an something fails to get marked as unprocessed the
// constraint is maintained that if an ad is processed, all older ads are also
// processed.
func (ing *Ingester) markAdUnprocessed(adCid cid.Cid) error {
	return ing.ds.Put(context.Background(), datastore.NewKey(adProcessedPrefix+adCid.String()), []byte{0})
}

func (ing *Ingester) adAlreadyProcessed(adCid cid.Cid) bool {
	v, err := ing.ds.Get(context.Background(), datastore.NewKey(adProcessedPrefix+adCid.String()))
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorw("Failed to read advertisement processed state from datastore", "err", err)
		}
		return false
	}
	return v[0] == byte(1)
}

func (ing *Ingester) markAdProcessed(publisher peer.ID, adCid cid.Cid) error {
	log.Debugw("Persisted latest sync", "peer", publisher, "cid", adCid)
	err := ing.ds.Put(context.Background(), datastore.NewKey(adProcessedPrefix+adCid.String()), []byte{1})
	if err != nil {
		return err
	}
	// This ad is processed, so remove it from the datastore.
	err = ing.ds.Delete(context.Background(), datastore.NewKey(adCid.String()))
	if err != nil {
		// Log the error, but do not return. Continue on to save the procesed ad.
		log.Errorw("Cound not remove advertisement from datastore", "err", err)
	}
	return ing.ds.Put(context.Background(), datastore.NewKey(syncPrefix+publisher.String()), adCid.Bytes())
}

// distributeEvents reads a adProcessedEvent, sent by a peer handler, and
// copies the event to all channels in outEventsChans. This delivers the event
// to all onAdProcessed channel readers.
func (ing *Ingester) distributeEvents() {
	for event := range ing.inEvents {
		// Send update to all change notification channels.
		ing.outEventsMutex.Lock()
		outEventsChans, ok := ing.outEventsChans[event.publisher]
		if ok {
			for _, ch := range outEventsChans {
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
	// Channel is buffered to prevent distribute() from blocking if a reader is
	// not reading the channel immediately.
	ch := make(chan adProcessedEvent, 1)
	ing.outEventsMutex.Lock()
	defer ing.outEventsMutex.Unlock()

	var outEventsChans []chan adProcessedEvent
	if ing.outEventsChans == nil {
		ing.outEventsChans = make(map[peer.ID][]chan adProcessedEvent)
	} else {
		outEventsChans = ing.outEventsChans[peerID]
	}
	ing.outEventsChans[peerID] = append(outEventsChans, ch)

	cncl := func() {
		ing.outEventsMutex.Lock()
		defer ing.outEventsMutex.Unlock()
		outEventsChans, ok := ing.outEventsChans[peerID]
		if !ok {
			return
		}

		for i, ca := range outEventsChans {
			if ca == ch {
				if len(outEventsChans) == 1 {
					if len(ing.outEventsChans) == 1 {
						ing.outEventsChans = nil
					} else {
						delete(ing.outEventsChans, peerID)
					}
				} else {
					outEventsChans[i] = outEventsChans[len(outEventsChans)-1]
					outEventsChans[len(outEventsChans)-1] = nil
					outEventsChans = outEventsChans[:len(outEventsChans)-1]
					close(ch)
					ing.outEventsChans[peerID] = outEventsChans
				}
				break
			}
		}
	}
	return ch, cncl
}

// signalMetricsUpdate signals that metrics should be updated.
func (ing *Ingester) signalMetricsUpdate() {
	select {
	case ing.sigUpdate <- struct{}{}:
	default:
		// Already signaled
	}
}

// metricsUpdate periodically updates metrics. This goroutine exits when the
// sigUpdate channel is closed, when Close is called.
func (ing *Ingester) metricsUpdater() {
	hasUpdate := true
	t := time.NewTimer(time.Minute)

	for {
		select {
		case _, ok := <-ing.sigUpdate:
			if !ok {
				return
			}
			hasUpdate = true
		case <-t.C:
			if hasUpdate {
				// Update value store size metric after sync.
				size, err := ing.indexer.Size()
				if err != nil {
					log.Errorw("Error getting indexer value store size", "err", err)
					return
				}
				stats.Record(context.Background(), coremetrics.StoreSize.M(size))
				hasUpdate = false
			}
			t.Reset(time.Minute)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for provInfo := range ing.reg.SyncChan() {
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

		// If a separate goroutine, attempt to sync the provider at its last
		// know publisher.
		ing.waitForPendingSyncs.Add(1)
		go func(pubID peer.ID, pubAddr multiaddr.Multiaddr, provID peer.ID) {
			defer ing.waitForPendingSyncs.Done()

			log := log.With("provider", provID, "publisher", pubID, "addr", pubAddr)
			log.Info("Auto-syncing the latest advertisement with publisher")

			_, err := ing.sub.Sync(ctx, pubID, cid.Undef, nil, pubAddr)
			if err != nil {
				log.Errorw("Failed to auto-sync with publisher", "err", err)
				return
			}
		}(provInfo.Publisher, provInfo.PublisherAddr, provInfo.AddrInfo.ID)
	}
}

// Get the latest CID synced for the peer.
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

func (ing *Ingester) BatchSize() int {
	return int(atomic.LoadUint32(&ing.batchSize))
}

func (ing *Ingester) SetBatchSize(batchSize int) {
	atomic.StoreUint32(&ing.batchSize, uint32(batchSize))
}

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
		go ing.ingestWorker(ing.workersCtx)
		ing.workerPoolSize++
	}
	for n < ing.workerPoolSize {
		// Stop worker.
		ing.closeWorkers <- struct{}{}
		ing.workerPoolSize--
	}
}

func (ing *Ingester) runIngesterLoop() {
	for syncFinishedEvent := range ing.toStaging {
		ing.runIngestStep(syncFinishedEvent)
	}
}

func (ing *Ingester) runIngestStep(syncFinishedEvent legs.SyncFinished) {
	log := log.With("publisher", syncFinishedEvent.PeerID)
	// 1. Group the incoming CIDs by provider.
	adsGroupedByProvider := map[peer.ID][]adInfo{}
	for _, c := range syncFinishedEvent.SyncedCids {
		// Group the CIDs by the provider. Most of the time a publisher will
		// only publish Ads for one provider, but it's possible that an ad
		// chain can include multiple providers.

		if ing.adAlreadyProcessed(c) {
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

		adsGroupedByProvider[providerID] = append(adsGroupedByProvider[providerID], adInfo{
			cid: c,
			ad:  ad,
		})
	}

	// 2. For each provider put the ad stack to the worker msg channel.
	for p, adInfos := range adsGroupedByProvider {
		ing.providersBeingProcessedMu.Lock()
		if _, ok := ing.providersBeingProcessed[p]; !ok {
			ing.providersBeingProcessed[p] = make(chan struct{}, 1)
		}
		wa, ok := ing.providerAdChainStaging[p]
		if !ok {
			wa = &atomic.Value{}
			ing.providerAdChainStaging[p] = wa
		}
		ing.providersBeingProcessedMu.Unlock()

		oldAssignment := wa.Swap(workerAssignment{
			adInfos:   adInfos,
			publisher: syncFinishedEvent.PeerID,
			provider:  p,
		})

		if oldAssignment == nil || oldAssignment.(workerAssignment).none {
			// No previous run scheduled a worker to handle this provider, so
			// schedule one.
			ing.reg.Saw(p)
			pushCount := ing.toWorkers.Push(providerID(p))
			stats.Record(context.Background(), metrics.AdIngestQueued.M(int64(ing.toWorkers.Length())))
			stats.Record(context.Background(), metrics.AdIngestBacklog.M(int64(pushCount)))
		}
	}
}

func (ing *Ingester) ingestWorker(ctx context.Context) {
	log.Debug("started ingest worker")
	defer ing.waitForWorkers.Done()

	for {
		select {
		case <-ing.closeWorkers:
			log.Debug("stopped ingest worker")
			return
		case provider := <-ing.toWorkers.PopChan():
			stats.Record(context.Background(), metrics.AdIngestQueued.M(int64(ing.toWorkers.Length())))
			pid := peer.ID(provider)
			ing.providersBeingProcessedMu.Lock()
			pc := ing.providersBeingProcessed[pid]
			ing.providersBeingProcessedMu.Unlock()
			pc <- struct{}{}
			ing.ingestWorkerLogic(ctx, pid)
			ing.handlePendingAnnounce(ctx, pid)
			<-pc
		}
	}
}

func (ing *Ingester) ingestWorkerLogic(ctx context.Context, provider peer.ID) {
	// Pull out the assignment for this provider. Note that runIngestStep
	// populates this atomic.Value.
	ing.providersBeingProcessedMu.Lock()
	wa := ing.providerAdChainStaging[provider]
	ing.providersBeingProcessedMu.Unlock()

	assignmentInterface := wa.Swap(workerAssignment{none: true})
	if assignmentInterface == nil || assignmentInterface.(workerAssignment).none {
		// Note this is here for completeness. This would not happen
		// normally. Execution could get here if someone manually calls this
		// function outside the ingest loop. Nothing to do – no assignment.
		return
	}
	assignment := assignmentInterface.(workerAssignment)

	rmCtxID := make(map[string]struct{})
	var skips []int
	skip := -1

	// Filter out ads that are already processed, and any earlier ads.
	splitAtIndex := len(assignment.adInfos)
	for i, ai := range assignment.adInfos {
		if ctx.Err() != nil {
			log.Infow("Ingest worker canceled while ingesting ads", "provider", provider, "err", ctx.Err())
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: assignment.adInfos[0].cid,
				adCid:     ai.cid,
				err:       ctx.Err(),
			}
			return
		}
		// Iterate latest to earliest.
		if ing.adAlreadyProcessed(ai.cid) {
			// This ad is already processed, which means that all earlier ads
			// are also processed. Break here and split at this index
			// later. The cids before this index are newer and have not been
			// processed yet; the cids after are older and have already been
			// processed.
			splitAtIndex = i
			break
		}
		ctxIdStr := string(ai.ad.ContextID)
		// This ad was deleted by a later remove. Push previous onto skips
		// stack, and set latest skip.
		if _, ok := rmCtxID[ctxIdStr]; ok {
			skips = append(skips, skip)
			skip = i
			continue
		}
		// If this is a remove, do not skip this ad, but skip all earlier
		// (deeped in chain) ads with the same context ID.
		if ai.ad.IsRm {
			rmCtxID[ctxIdStr] = struct{}{}
		}
	}

	log.Infow("Running worker on ad stack", "headAdCid", assignment.adInfos[0].cid, "publisher", assignment.publisher, "numAdsToProcess", splitAtIndex)
	var count int
	for i := splitAtIndex - 1; i >= 0; i-- {
		// Note that iteration proceeds backwards here. Earliest to newest.
		ai := assignment.adInfos[i]
		count++

		if ctx.Err() != nil {
			log.Infow("Ingest worker canceled while processing ads", "provider", provider, "err", ctx.Err())
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: assignment.adInfos[0].cid,
				adCid:     ai.cid,
				err:       ctx.Err(),
			}
			return
		}

		// If this ad is skipped because it gets deleted later in the chain,
		// then mark this ad as processed.
		if i == skip {
			// Pop the next skip off the stack.
			skip = skips[len(skips)-1]
			skips = skips[:len(skips)-1]
			log.Infow("Skipping advertisement with deleted context",
				"adCid", ai.cid,
				"publisher", assignment.publisher,
				"progress", fmt.Sprintf("%d of %d", count, splitAtIndex))

			if markErr := ing.markAdProcessed(assignment.publisher, ai.cid); markErr != nil {
				log.Errorw("Failed to mark ad as processed", "err", markErr)
			}
			// Distribute the atProcessedEvent notices to waiting Sync calls.
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: assignment.adInfos[0].cid,
				adCid:     ai.cid,
			}
			continue
		}

		log.Infow("Processing advertisement",
			"adCid", ai.cid,
			"publisher", assignment.publisher,
			"progress", fmt.Sprintf("%d of %d", count, splitAtIndex))

		err := ing.ingestAd(assignment.publisher, ai.cid, ai.ad)
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
			log.Errorw("Error while ingesting ad. Bailing early, not ingesting later ads.", "adCid", ai.cid, "publisher", assignment.provider, "err", err, "adsLeftToProcess", i+1)

			// Tell anyone waiting that the sync finished for this head because
			// of error.  TODO(mm) would be better to propagate the error.
			ing.inEvents <- adProcessedEvent{
				publisher: assignment.publisher,
				headAdCid: assignment.adInfos[0].cid,
				adCid:     ai.cid,
				err:       err,
			}
			return
		}

		if markErr := ing.markAdProcessed(assignment.publisher, ai.cid); markErr != nil {
			log.Errorw("Failed to mark ad as processed", "err", markErr)
		}
		// Distribute the atProcessedEvent notices to waiting Sync calls.
		ing.inEvents <- adProcessedEvent{
			publisher: assignment.publisher,
			headAdCid: assignment.adInfos[0].cid,
			adCid:     ai.cid,
		}
	}
}

func (ing *Ingester) handlePendingAnnounce(ctx context.Context, pid peer.ID) {
	if ctx.Err() != nil {
		return
	}
	log := log.With("provider", pid)
	// Process pending announce request if any.
	// Note that the pending announce  is deleted regardless of whether it was successfully
	// processed or not. Because, the cause of failure may be non-recoverable e.g. address
	// change and not removing it will block processing of future pending announces.
	v, found := ing.providersPendingAnnounce.LoadAndDelete(pid)
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
