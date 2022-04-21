package ingest

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer/ingest")

// prefix used to track latest sync in datastore.
const (
	syncPrefix  = "/sync/"
	admapPrefix = "/admap/"
	// This prefix represents all the ads we've already processed.
	adProcessedPrefix = "/adProcessed/"

	// Absolute maximum number of entries an advertisement can have. Should
	// only effect misconfigured publisher.
	entriesDepthLimit = 16384
)

type adProcessedEvent struct {
	publisher peer.ID
	// The head of the chain we're proecessing.
	headAdCid cid.Cid
	// The actual adCid we're processing.
	adCid cid.Cid
	// if not nil we failed to process the ad for adCid
	err error
}

type providerID peer.ID

// Ingester is a type that uses go-legs for the ingestion protocol.
type Ingester struct {
	host    host.Host
	ds      datastore.Batching
	indexer indexer.Interface

	batchSize int
	closeOnce sync.Once
	sigUpdate chan struct{}

	sub         *legs.Subscriber
	syncTimeout time.Duration

	entriesSel datamodel.Node
	reg        *registry.Registry

	cfg config.Ingest

	// inEvents is used to send a adProcessedEvent to the distributeEvents
	// goroutine, when an advertisement in marked complete or err'd.
	inEvents chan adProcessedEvent

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of an adProccedEvent to an onAdProcessed reader.
	outEventsChans map[peer.ID][]chan adProcessedEvent
	outEventsMutex sync.Mutex

	waitForPendingSyncs sync.WaitGroup
	closePendingSyncs   chan struct{}

	cancelOnSyncFinished context.CancelFunc

	// A map of providers currently being processed. A worker holds the lock of a
	// provider while ingesting ads for that provider.
	providersBeingProcessed   map[peer.ID]*sync.Mutex
	providersBeingProcessedMu sync.Mutex
	providerAdChainStaging    map[peer.ID]*atomic.Value
	// toWorkers is a channel used to ask the worker pool to start processing the ad chain for a given provider
	toWorkers      chan providerID
	closeWorkers   chan struct{}
	waitForWorkers sync.WaitGroup
	toStaging      <-chan legs.SyncFinished
}

// NewIngester creates a new Ingester that uses a go-legs Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr indexer.Interface, reg *registry.Registry, ds datastore.Batching) (*Ingester, error) {
	lsys := mkLinkSystem(ds, reg)

	// Construct a selector that recursively looks for nodes with field
	// "PreviousID" as per Advertisement schema.  Note that the entries within
	// an advertisement are synced separately triggered by storage hook, so
	// that we can check if a chain of chunks exist already before syncing it.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}).Node()

	// Construct the selector used when syncing entries of an advertisement
	// with non-configurable recursion limit.
	entSel := ssb.ExploreRecursive(selector.RecursionLimitDepth(entriesDepthLimit),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Next", ssb.ExploreRecursiveEdge()) // Next field in EntryChunk
		})).Node()

	ing := &Ingester{
		host:        h,
		ds:          ds,
		indexer:     idxr,
		batchSize:   cfg.StoreBatchSize,
		sigUpdate:   make(chan struct{}, 1),
		syncTimeout: time.Duration(cfg.SyncTimeout),
		entriesSel:  entSel,
		reg:         reg,
		cfg:         cfg,
		inEvents:    make(chan adProcessedEvent, 1),

		closePendingSyncs: make(chan struct{}),

		providersBeingProcessed: make(map[peer.ID]*sync.Mutex),
		providerAdChainStaging:  make(map[peer.ID]*atomic.Value),
		toWorkers:               make(chan providerID),
		closeWorkers:            make(chan struct{}),
	}

	// Create and start pubsub subscriber.  This also registers the storage
	// hook to index data as it is received.
	sub, err := legs.NewSubscriber(h, ds, lsys, cfg.PubSubTopic, adSel, legs.AllowPeer(reg.Allowed), legs.UseLatestSyncHandler(&syncHandler{ing}))
	if err != nil {
		log.Errorw("Failed to start pubsub subscriber", "err", err)
		return nil, errors.New("ingester subscriber failed")
	}
	ing.sub = sub

	ing.toStaging, ing.cancelOnSyncFinished = ing.sub.OnSyncFinished()

	if cfg.IngestWorkerCount == 0 {
		return nil, errors.New("ingester worker count must be > 0")
	}
	ing.startIngesterLoop(cfg.IngestWorkerCount)

	// Start distributor to send SyncFinished messages to interested parties.
	go ing.distributeEvents()

	go ing.metricsUpdater()

	go ing.autoSync()

	log.Debugf("Ingester started and all hooks and linksystem registered")

	return ing, nil
}

func (ing *Ingester) Close() error {
	// Close leg transport.
	err := ing.sub.Close()

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
		close(ing.closePendingSyncs)
		ing.waitForPendingSyncs.Wait()

		// Stop the distribution goroutine.
		close(ing.inEvents)

		close(ing.sigUpdate)
	})

	return err
}

// Sync syncs advertisements, up to the the latest advertisement, from a
// publisher.  A channel is returned that gives the caller the option to wait
// for Sync to complete.  The channel returns the final CID that was synced by
// the call to Sync.
//
// Sync works by first fetching each advertisement from the specidief peer
// starting at the most recent and traversing to the advertisement last seen by
// the indexer, or until the advertisement depth limit is reached.  Then the
// entries in each advertisement are synced and the multihashes in each entry
// are indexed.
//
// The selector used to sync the advertisement is controlled by the following
// parameters: depth, and resync.
//
// The depth argument specifies the recursion depth limit to use during sync.
// Its value may less than 1 for no limit, or greater than 1 for an explicit
// limit.
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
// The Context argument controls the lifetime of the sync.  Canceling it
// cancels the sync and causes the multihash channel to close without any data.
func (ing *Ingester) Sync(ctx context.Context, peerID peer.ID, peerAddr multiaddr.Multiaddr, depth int64, resync bool) (<-chan cid.Cid, error) {
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
		// If depth is non-zero or traversal should not stop at the latest synced, then construct a
		// selector to behave accordingly.
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
		// asynchronously.  Sync with cid.Undef so that the latest head is
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
			// of this sync.  Create a scoped block-hook to do this.
			opts = append(opts, legs.ScopedBlockHook(func(i peer.ID, c cid.Cid) {
				err := ing.markAdUnprocessed(c)
				if err != nil {
					log.Errorw("Failed to mark ad as unprocessed", "err", err, "adCid", c)
				}
			}))
		}
		c, err := ing.sub.Sync(ctx, peerID, cid.Undef, sel, peerAddr, opts...)
		if err != nil {
			log.Errorw("Failed to sync with provider", "err", err)
			return
		}
		// Do not persist the latest sync here, because that is done in
		// after we've processed the ad.

		// If latest head had already finished syncing, then do not wait
		// for syncDone since it will never happen.
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
					// If we had an error than the adProcessedEvent.adCid will be the cid
					// that caused the error, and we won't get any future
					// adProcessedEvents. Therefore we check the headAdCid to see if this
					// was the sync we started.
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

// Announce send an annouce message to directly to go-legs, instead of through pubsub.
func (ing *Ingester) Announce(ctx context.Context, nextCid cid.Cid, addrInfo peer.AddrInfo) error {
	log.Infow("Handling direct announce request", "peer", addrInfo.ID)
	return ing.sub.Announce(ctx, nextCid, addrInfo.ID, addrInfo.Addrs)
}

func (ing *Ingester) makeLimitedDepthSelector(peerID peer.ID, depth int64, resync bool) (ipld.Node, error) {
	// Consider the value of < 1 as no-limit.
	var rLimit selector.RecursionLimit
	if depth < 1 {
		rLimit = selector.RecursionLimitNone()
	} else {
		rLimit = selector.RecursionLimitDepth(depth)
	}
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
	log = log.With("stopAt", stopAt)

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSequence := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}).Node()

	log.Debug("Custom selector constructed for explicit sync")
	return legs.ExploreRecursiveWithStopNode(rLimit, adSequence, stopAt), nil
}

// markAdUnprocessed takes an advertisement CID and marks it as
// unprocessed. This lets the ad be re-ingested in case we want to re-ingest
// with different depths or are processing even earlier ads and need to
// reprocess later ones so that the indexer re-ingests the later ones in the
// context of the earlier ads, and thus become consistent.
//
// During a sync, this should be called be in order from newest to oldest
// ad. This is so that if an something fails to get marked as unprocessed we
// still hold the constraint that if an ad is processed, all older ads are also
// processed.
func (ing *Ingester) markAdUnprocessed(adCid cid.Cid) error {
	return ing.ds.Put(context.Background(), datastore.NewKey(adProcessedPrefix+adCid.String()), []byte{0})
}

func (ing *Ingester) adAlreadyprocessed(adCid cid.Cid) bool {
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
	// We've processed this ad, so we can remove it from our datastore.
	ing.ds.Delete(context.Background(), dsKey(adCid.String()))
	return ing.ds.Put(context.Background(), datastore.NewKey(syncPrefix+publisher.String()), adCid.Bytes())
}

// distributeEvents reads a adProcessedEvent, sent by a peer handler, and copies
// the event to all channels in outEventsChans. This delivers the event to all
// onAdProcessed channel readers.
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

// metricsUpdate periodically updates metrics.  This goroutine exits when the
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

func (ing *Ingester) autoSync() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for provInfo := range ing.reg.SyncChan() {
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
func (ing *Ingester) GetLatestSync(peerID peer.ID) (cid.Cid, error) {
	b, err := ing.ds.Get(context.Background(), datastore.NewKey(syncPrefix+peerID.String()))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

type adInfo struct {
	cid cid.Cid
	ad  schema.Advertisement
}

type workerAssignment struct {
	// none represents a nil assignment. Used because we can't store nil in atomic.Value.
	none      bool
	adInfos   []adInfo
	publisher peer.ID
	provider  peer.ID
}

func (ing *Ingester) startIngesterLoop(workerPoolSize int) {
	// startup the worker pool
	for i := 0; i < workerPoolSize; i++ {
		ing.waitForWorkers.Add(1)
		go func() {
			defer ing.waitForWorkers.Done()
			ing.ingestWorker()
		}()
	}

	go func() {
		for syncFinishedEvent := range ing.toStaging {
			ing.runIngestStep(syncFinishedEvent)
		}
	}()
}

func (ing *Ingester) runIngestStep(syncFinishedEvent legs.SyncFinished) {
	// 1. Group the incoming CIDs by provider.
	adsGroupedByProvider := map[peer.ID][]adInfo{}
	for _, c := range syncFinishedEvent.SyncedCids {
		// Group the CIDs by the provider. Most of the time a publisher will only
		// publish Ads for one provider, but it's possible that an ad chain can include multiple providers.

		if ing.adAlreadyprocessed(c) {
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
			ing.providersBeingProcessed[p] = &sync.Mutex{}
		}
		if _, ok := ing.providerAdChainStaging[p]; !ok {
			ing.providerAdChainStaging[p] = &atomic.Value{}
		}
		ing.providersBeingProcessedMu.Unlock()

		oldAssignment := ing.providerAdChainStaging[p].Swap(workerAssignment{
			adInfos:   adInfos,
			publisher: syncFinishedEvent.PeerID,
			provider:  p,
		})

		if oldAssignment == nil || oldAssignment.(workerAssignment).none {
			// No previous run scheduled a worker to handle this provider, so let's
			// schedule one
			ing.toWorkers <- providerID(p)
		}
	}
}

func (ing *Ingester) ingestWorker() {
	for {
		select {
		case <-ing.closeWorkers:
			return
		case provider := <-ing.toWorkers:
			ing.ingestWorkerLogic(peer.ID(provider))
		}
	}
}

func (ing *Ingester) ingestWorkerLogic(provider peer.ID) {

	// It's assumed that the runIngestStep puts a mutex in this map.
	ing.providersBeingProcessed[provider].Lock()
	defer ing.providersBeingProcessed[provider].Unlock()

	// Pull out the assignment for this provider. Note that runIngestStep populates this atomic.Value.
	assignmentInterface := ing.providerAdChainStaging[provider].Swap(workerAssignment{none: true})
	if assignmentInterface == nil || assignmentInterface.(workerAssignment).none {
		// Note this is here for completeness. This wouldn't happen normally.  We
		// could get here if someone manually calls this function outside the ingest
		// loop.
		// Nothing to do – no assignment.
		return
	}
	assignment := assignmentInterface.(workerAssignment)

	// Filter out ads that we've already processed and any earlier ads
	splitAtIndex := len(assignment.adInfos)
	for i, ai := range assignment.adInfos {
		// iterate latest to earliest
		if ing.adAlreadyprocessed(ai.cid) {
			// We've process this ad already, so we know we've processed all
			// earlier ads too. Break here and split at this index later. The cids
			// before this index are newer and we haven't processed them, the cids
			// after are older and we already have processed them.
			splitAtIndex = i
			break
		}
	}

	log.Infow("Running worker on ad stack", "headAdCid", assignment.adInfos[0].cid, "publisher", assignment.publisher, "numAdsToProcess", splitAtIndex)
	var count int
	for i := splitAtIndex - 1; i >= 0; i-- {
		// Note that we are iterating backwards here. Earliest to newest.
		ai := assignment.adInfos[i]

		count++
		log.Infow("Processing advertisement",
			"adCid", ai.cid,
			"publisher", assignment.publisher,
			"progress", fmt.Sprintf("%d of %d", count, splitAtIndex))

		err := ing.ingestAd(assignment.publisher, ai.cid, ai.ad)

		if err == nil {
			// No error at all, we processed this ad successfully.
			stats.Record(context.Background(), metrics.AdIngestSuccessCount.M(1))
		}

		var adIngestErr adIngestError
		if errors.As(err, &adIngestErr) {
			switch adIngestErr.state {
			case adIngestDecodingErr, adIngestMalformedErr, adIngestEntryChunkErr, adIngestContentNotFound:
				// These error cases are permament. e.g. if we try again later we will hit the same error. So we log and drop this error.
				log.Errorw("Skipping ad because of a permanant error", "adCid", ai.cid, "err", err, "errKind", adIngestErr.state)
				err = nil
				stats.Record(context.Background(), metrics.AdIngestSkippedCount.M(1))
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

			// Tell anyone who's waiting that the sync finished for this head because we errored.
			// TODO(mm) would be better to propagate the error.
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
			err:       nil,
		}
	}
}
