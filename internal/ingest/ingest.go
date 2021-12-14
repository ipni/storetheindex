package ingest

import (
	"context"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core/engine"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/gammazero/keymutex"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer/ingest")

var (
	_ LegIngester = &legIngester{}
)

// prefix used to track latest sync in datastore.
const (
	syncPrefix  = "/sync/"
	admapPrefix = "/admap/"
)

// LegIngester interface
type LegIngester interface {
	Ingester
	Close(context.Context) error
}

// legIngester is an ingester type that leverages go-legs for the
// ingestion protocol.
type legIngester struct {
	host    host.Host
	ds      datastore.Batching
	lms     legs.LegMultiSubscriber
	indexer *indexer.Engine

	subs  map[peer.ID]*subscriber
	sublk *keymutex.KeyMutex

	batchSize int
	sigUpdate chan struct{}
}

// subscriber datastructure for a peer.
type subscriber struct {
	peerID  peer.ID
	ls      legs.LegSubscriber
	watcher <-chan cid.Cid
	cncl    context.CancelFunc
}

// NewLegIngester creates a new go-legs-backed ingester.
func NewLegIngester(ctx context.Context, cfg config.Ingest, h host.Host,
	idxr *indexer.Engine, reg *registry.Registry, ds datastore.Batching) (LegIngester, error) {

	lsys := mkLinkSystem(ds, reg)

	// Construct a selector that recursively looks for nodes with field
	// "PreviousID" as per Advertisement schema.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	advAndChunkSel := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Entries", ssb.ExploreRecursiveEdge())
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
			efsb.Insert("Next", ssb.ExploreRecursiveEdge())
		}).Node()

	lms, err := legs.NewMultiSubscriber(ctx, h, ds, lsys, cfg.PubSubTopic, advAndChunkSel)
	if err != nil {
		log.Errorf("Failed to state LegTransport in ingester: %s", err)
		return nil, err
	}

	li := &legIngester{
		host:      h,
		ds:        ds,
		indexer:   idxr,
		lms:       lms,
		subs:      make(map[peer.ID]*subscriber),
		sublk:     keymutex.New(0),
		batchSize: cfg.StoreBatchSize,
		sigUpdate: make(chan struct{}, 1),
	}

	// Register storage hook to index data as we receive it.
	lms.GraphSync().RegisterIncomingBlockHook(li.storageHook())
	log.Debugf("LegIngester started and all hooks and linksystem registered")

	go li.metricsUpdater()

	return li, nil
}

// metricsUpdate periodically updates metrics.  This goroutine exits when the
// sigUpdate channel is closed, when Close is called.
func (li *legIngester) metricsUpdater() {
	var hasUpdate bool
	t := time.NewTimer(time.Minute)

	for {
		select {
		case _, ok := <-li.sigUpdate:
			if !ok {
				return
			}
			hasUpdate = true
		case <-t.C:
			if hasUpdate {
				// Update value store size metric after sync.
				size, err := li.indexer.Size()
				if err != nil {
					log.Errorf("Error getting indexer value store size: %s", err)
					return
				}
				stats.Record(context.Background(), coremetrics.StoreSize.M(size))
				hasUpdate = false
			}
			t.Reset(time.Minute)
		}
	}
}

// Sync with a data provider up to latest ID.
func (li *legIngester) Sync(ctx context.Context, peerID peer.ID, opts ...SyncOption) (<-chan multihash.Multihash, error) {
	log.Debugf("Syncing with peer %s", peerID)

	// Get subscriber for peer or create a new one
	sub, err := li.newPeerSubscriber(ctx, peerID)
	if err != nil {
		log.Errorf("Error getting a subscriber instance for provider: %s", err)
		return nil, err
	}

	// Apply options to syncConfig or use defaults
	var cfg SyncConfig
	if err := cfg.Apply(append([]SyncOption{SyncDefaults}, opts...)...); err != nil {
		return nil, err
	}

	// Configure timeout for syncing process
	ctx, cancel := context.WithTimeout(ctx, cfg.SyncTimeout)
	// Start syncing. Notifications for the finished
	// sync will be done asynchronously.
	log.Debugf("Started syncing process with provider %s", sub)

	// Sync with cid.Undef and nil selector so that:
	//   1. the latest head is queried by go-legs via head-publisher
	//   2. the default selector is used where traversal stops at the latest known head.
	watcher, cncl, err := sub.ls.Sync(ctx, peerID, cid.Undef, nil)
	if err != nil {
		log.Errorf("Errored while syncing: %s", err)
		cancel()
		return nil, err
	}
	// Merge cancelfuncs
	cncl = cancelFunc(cncl, cancel)
	// Notification channel; buffered so as not to block if no reader.
	out := make(chan multihash.Multihash, 1)
	// Listen when the sync is done to update latestSync and notify the
	// channel. No need to pass ctx here, because if ctx is canceled, then
	// watcher is closed.
	go li.listenSyncUpdate(peerID, watcher, cncl, out)
	return out, nil
}

// Subscribe to advertisements of a specific provider in the pubsub channel.
func (li *legIngester) Subscribe(ctx context.Context, peerID peer.ID) error {
	log.Infow("Subscribing to advertisement pub-sub channel", "host_id", peerID)
	sctx, cancel := context.WithCancel(ctx)
	sub, err := li.newPeerSubscriber(sctx, peerID)
	if err != nil {
		log.Errorf("Error getting a subscriber instance for provider: %s", err)
		cancel()
		return err
	}

	// If already subscribed do nothing.
	if sub.watcher != nil {
		log.Infow("Already subscribed to provider", "id", peerID)
		cancel()
		return nil
	}

	var cncl context.CancelFunc
	sub.watcher, cncl = sub.ls.OnChange()
	sub.cncl = cancelFunc(cncl, cancel)

	// Listen updates persist latestSync when sync is done.
	go li.listenSubUpdates(sub)
	return nil
}

func (li *legIngester) listenSubUpdates(sub *subscriber) {
	for c := range sub.watcher {
		// Persist the latest sync
		if err := li.putLatestSync(sub.peerID, c); err != nil {
			log.Errorf("Error persisting latest sync: %s", err)
		}
	}
}

func (li *legIngester) listenSyncUpdate(peerID peer.ID, watcher <-chan cid.Cid, cncl context.CancelFunc, out chan<- multihash.Multihash) {
	defer func() {
		cncl()
		close(out)
	}()

	startTime := time.Now()

	log.Infof("Waiting for sync to finish for provider %s", peerID)
	c, ok := <-watcher
	if ok {
		// Persist the latest sync
		err := li.putLatestSync(peerID, c)
		if err != nil {
			log.Errorf("Error persisting latest sync: %s", err)
		}
		out <- c.Hash()

		stats.Record(context.Background(), metrics.SyncLatency.M(coremetrics.MsecSince(startTime)))
		li.sigUpdate <- struct{}{}
	}
}

// Unsubscribe to stop listening to advertisement from a specific provider.
func (li *legIngester) Unsubscribe(ctx context.Context, peerID peer.ID) error {
	log.Debugf("Unsubscribing from provider %s", peerID)
	li.sublk.Lock(string(peerID))
	defer li.sublk.Unlock(string(peerID))
	// Check if subscriber exists.
	sub, ok := li.subs[peerID]
	if !ok {
		log.Infof("Not subscribed to provider %s. Nothing to do", peerID)
		// If not we have nothing to do.
		return nil
	}
	// Close subscriber
	sub.ls.Close()
	// Check if we are subscribed
	if sub.cncl != nil {
		// If yes, run cancel
		sub.cncl()
	}
	// Delete from map
	delete(li.subs, peerID)
	log.Infof("Unsubscribed from provider %s successfully", peerID)

	return nil
}

// Creates a new subscriber for a peer according to its latest sync.
func (li *legIngester) newPeerSubscriber(ctx context.Context, peerID peer.ID) (*subscriber, error) {
	li.sublk.Lock(string(peerID))
	defer li.sublk.Unlock(string(peerID))
	sub, ok := li.subs[peerID]
	// If there is already a subscriber for the peer, do nothing.
	if ok {
		return sub, nil
	}

	// See if we already synced with this peer.
	c, err := li.getLatestSync(peerID)
	if err != nil {
		return nil, err
	}

	// TODO: Make a request to provider to see if it has any new advertisement
	// and sync before initializing subscriber?

	// If not synced start a brand new subscriber
	var ls legs.LegSubscriber
	if c == cid.Undef {
		ls, err = li.lms.NewSubscriber(legs.FilterPeerPolicy(peerID))
	} else {
		// If yes, start a partially synced subscriber.
		ls, err = li.lms.NewSubscriberPartiallySynced(legs.FilterPeerPolicy(peerID), c)
	}
	if err != nil {
		return nil, err
	}
	sub = &subscriber{
		peerID: peerID,
		ls:     ls,
	}
	li.subs[peerID] = sub
	return sub, nil
}

func (li *legIngester) Close(ctx context.Context) error {
	// Unsubscribe from all peers
	for k := range li.subs {
		err := li.Unsubscribe(ctx, k)
		if err != nil {
			return err
		}
	}
	// Close leg transport.
	err := li.lms.Close(ctx)
	close(li.sigUpdate)
	return err
}

// Get the latest cid synced for the peer.
func (li *legIngester) getLatestSync(peerID peer.ID) (cid.Cid, error) {
	b, err := li.ds.Get(datastore.NewKey(syncPrefix + peerID.String()))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

// Tracks latest sync for a specific peer.
func (li *legIngester) putLatestSync(peerID peer.ID, c cid.Cid) error {
	// Do not save if empty CIDs are received. Closing the channel
	// may lead to receiving empty CIDs.
	if c == cid.Undef {
		return nil
	}
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "libp2p2")),
		stats.WithMeasurements(metrics.IngestChange.M(1)))

	return li.ds.Put(datastore.NewKey(syncPrefix+peerID.String()), c.Bytes())
}

// cancelfunc for subscribers. Combines context cancel and LegSubscriber
// cancel function.
func cancelFunc(c1, c2 context.CancelFunc) context.CancelFunc {
	return func() {
		c1()
		c2()
	}
}
