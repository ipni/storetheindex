package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
)

var log = logging.Logger("indexer/ingest")

// prefix used to track latest sync in datastore.
const (
	syncPrefix  = "/sync/"
	admapPrefix = "/admap/"
)

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
	adWaiter    *cidWaiter

	adCache      map[cid.Cid]adCacheItem
	adCacheMutex sync.Mutex

	entriesSel datamodel.Node
	reg        *registry.Registry

	skips      map[string]struct{}
	skipsMutex sync.Mutex

	cfg config.Ingest

	// inEvents is used to send a legs.SyncFinished to the distributeEvents
	// goroutine, when an advertisement in marked complete.
	inEvents chan legs.SyncFinished

	// outEventsChans is a slice of channels, where each channel delivers a
	// copy of a legs.SyncFinished to an onAdProcessed reader.
	outEventsChans map[peer.ID][]chan cid.Cid
	outEventsMutex sync.Mutex
}

// NewIngester creates a new Ingester that uses a go-legs Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr indexer.Interface, reg *registry.Registry, ds datastore.Batching) (*Ingester, error) {
	// Cleanup any leftover entry cid to ad cid mappings.
	err := removeEntryAdMappings(context.Background(), ds)
	if err != nil {
		log.Errorw("Error cleaning temporary entries ad to mappings", "err", err)
		// Do not return error; keep going.
	}

	adWaiter := newCidWaiter()
	lsys := mkLinkSystem(ds, adWaiter)

	// Construct a selector that recursively looks for nodes with field
	// "PreviousID" as per Advertisement schema.  Note that the entries within
	// an advertisement are synced separately triggered by storage hook, so
	// that we can check if a chain of chunks exist already before syncing it.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		}).Node()

	// Construct the selector used when syncing entries of an advertisement with the configured
	// recursion limit.
	entSel := ssb.ExploreRecursive(cfg.EntriesRecursionLimit(),
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
		adWaiter:    adWaiter,
		entriesSel:  entSel,
		reg:         reg,
		cfg:         cfg,
		inEvents:    make(chan legs.SyncFinished, 1),
	}

	// Create and start pubsub subscriber.  This also registers the storage
	// hook to index data as it is received.
	sub, err := legs.NewSubscriber(h, ds, lsys, cfg.PubSubTopic, adSel, legs.AllowPeer(reg.Authorized), legs.BlockHook(ing.storageHook), legs.SyncRecursionLimit(cfg.AdvertisementRecursionLimit()))
	if err != nil {
		log.Errorw("Failed to start pubsub subscriber", "err", err)
		return nil, errors.New("ingester subscriber failed")
	}
	ing.sub = sub

	err = ing.restoreLatestSync()
	if err != nil {
		sub.Close()
		return nil, err
	}

	// TODO handle cancel
	onsyncFinishedChan, _ := ing.sub.OnSyncFinished()
	go func() {
		for syncFinished := range onsyncFinishedChan {
			// syncFinished.SyncedCids
			for i := len(syncFinished.SyncedCids) - 1; i >= 0; i-- {
				adCid := syncFinished.SyncedCids[i]
				node, err := lsys.Load(linking.LinkContext{}, cidlink.Link{Cid: adCid}, basicnode.Prototype.Any)
				if err != nil {
					fmt.Println("!!!!!!! Error loading advertisement:", err)
					continue
				}

				ad, err := decodeAd(node)
				if err != nil {
					fmt.Println("!!!!! Error decoding advertisement:", err)
					continue
				}

				fmt.Println("PROCESSING ADVERTISEMENT:", adCid)
				// cid.Undef so we avoid cidwaiter code
				ing.syncAdEntries(syncFinished.PeerID, ad, adCid, cid.Undef)
				fmt.Println("DONE PROCESSING ADVERTISEMENT:", adCid)
			}
		}

	}()

	// Start distributor to send SyncFinished messages to interested parties.
	go ing.distributeEvents()

	go ing.metricsUpdater()

	log.Debugf("Ingester started and all hooks and linksystem registered")

	return ing, nil
}

func (ing *Ingester) Close() error {
	ing.adWaiter.close()

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
		close(ing.sigUpdate)
		// Stop the distribution goroutine.
		close(ing.inEvents)
	})

	return err
}

// Sync syncs the latest advertisement from a publisher.  This is done by first
// fetching the latest advertisement ID from and traversing it until traversal
// gets to the last seen advertisement.  Then the entries in each advertisement
// are synced and the multihashes in each entry are indexed.
//
// The selector used to sync the advertisement is controlled by the following
// parameters: depth, and ignoreLatest.
//
// The depth argument specifies the recursion depth limit to use during sync.
// Its value may be one of: -1 for no-limit, 0 for same value as config.Ingest,
// or larger than 0 for an explicit limit.
//
// The ignoreLatest argument specifies whether to stop the traversal at the
// latest known advertisement that is already synced. If set to true, the
// traversal will continue until either there are no more advertisements left
// or the recursion depth limit is reached.
//
// Note that the reference to the latest synced advertisement returned by
// GetLatestSync is only updated if the given depth is zero and ignoreLatest
// is set to false. Otherwise, a custom selector with the given depth limit and
// stop link is constructed and used for traversal. See legs.Subscriber.Sync.
//
// The Context argument controls the lifetime of the sync.  Canceling it
// cancels the sync and causes the multihash channel to close without any data.
//
// Note that the multihash entries corresponding to the advertisement are
// synced in the background.  The completion of advertisement sync does not
// necessarily mean that the entries corresponding to the advertisement are
// synced.
func (ing *Ingester) Sync(ctx context.Context, peerID peer.ID, peerAddr multiaddr.Multiaddr, depth int64, ignoreLatest bool) (<-chan multihash.Multihash, error) {
	out := make(chan cid.Cid, 1)

	// Fail fast if peer ID or depth is invalid.
	if depth < -1 {
		return nil, fmt.Errorf("recursion depth limit must not be less than -1; got %d", depth)
	}
	if err := peerID.Validate(); err != nil {
		return nil, err
	}

	log := log.With("provider", peerID, "peerAddr", peerAddr, "depth", depth, "ignoreLatest", ignoreLatest)
	log.Debug("Explicitly syncing the latest advertisement from peer")

	go func() {
		defer close(out)

		var sel ipld.Node
		// If depth is non-zero or traversal should not stop at the latest synced, then construct a
		// selector to behave accordingly.
		if depth != 0 || ignoreLatest {
			var err error
			sel, err = ing.makeLimitedDepthSelector(peerID, depth, ignoreLatest)
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
		// asynchronously.  Sync with cid.Undef so that the latest head
		// is queried by go-legs via head-publisher.
		//
		// Note that if the selector is nil the default selector is used
		// where traversal stops at the latest known head.
		//
		// Reference to the latest synced CID is only updated if the given
		// selector is nil.
		c, err := ing.sub.Sync(ctx, peerID, cid.Undef, sel, peerAddr)
		if err != nil {
			log.Errorw("Failed to sync with provider", "err", err)
			return
		}
		// Do not persist the latest sync here, because that is done in
		// after we've processed the ad.

		// If latest head had already finished syncing, then do not wait for syncDone since it will never happen.
		if latest == c {
			log.Infow("Latest advertisement already processed", "adCid", c)
			out <- c
			return
		}

		log.Debugw("Syncing advertisements up to latest", "adCid", c)
		for {
			select {
			case adCid := <-syncDone:
				log.Debugw("Synced advertisement", "adCid", adCid)
				if adCid == c {
					out <- c
					ing.signalMetricsUpdate()
					return
				}
			case <-ctx.Done():
				log.Warnw("Sync cancelled", "err", ctx.Err())
				return
			}
		}
	}()
	return out, nil
}

func (ing *Ingester) makeLimitedDepthSelector(peerID peer.ID, depth int64, ignoreLatest bool) (ipld.Node, error) {
	// Consider the value of -1 as no-limit, similar to config.Ingest.
	var rLimit selector.RecursionLimit
	if depth == -1 {
		rLimit = selector.RecursionLimitNone()
	} else if depth == 0 {
		rLimit = ing.cfg.AdvertisementRecursionLimit()
		// Override the value of depth with config.Ingest value for logging purposes.
		depth = ing.cfg.AdvertisementDepthLimit
	} else {
		rLimit = selector.RecursionLimitDepth(depth)
	}
	log := log.With("depth", depth)

	var stopAt ipld.Link
	if !ignoreLatest {
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

func (ing *Ingester) markAdProcessed(publisher peer.ID, adCid cid.Cid) error {
	log.Debugw("Persisted latest sync", "peer", publisher, "cid", adCid)
	return ing.ds.Put(context.Background(), datastore.NewKey(syncPrefix+publisher.String()), adCid.Bytes())
}

// distributeEvents reads a SyncFinished, sent by a peer handler, and copies
// the even to all channels in outEventsChans.  This delivers the SyncFinished
// to all onAdProcessed channel readers.
func (ing *Ingester) distributeEvents() {
	for event := range ing.inEvents {
		// Send update to all change notification channels.
		ing.outEventsMutex.Lock()
		outEventsChans, ok := ing.outEventsChans[event.PeerID]
		if ok {
			for _, ch := range outEventsChans {
				ch <- event.Cid
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
func (ing *Ingester) onAdProcessed(peerID peer.ID) (<-chan cid.Cid, context.CancelFunc) {
	// Channel is buffered to prevent distribute() from blocking if a reader is
	// not reading the channel immediately.
	ch := make(chan cid.Cid, 1)
	ing.outEventsMutex.Lock()
	defer ing.outEventsMutex.Unlock()

	var outEventsChans []chan cid.Cid
	if ing.outEventsChans == nil {
		ing.outEventsChans = make(map[peer.ID][]chan cid.Cid)
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
					log.Errorf("Error getting indexer value store size: %w", err)
					return
				}
				stats.Record(context.Background(), coremetrics.StoreSize.M(size))
				hasUpdate = false
			}
			t.Reset(time.Minute)
		}
	}
}

// restoreLatestSync reads the latest sync for each previously synced provider,
// from the datastore, and sets this in the Subscriber.
func (ing *Ingester) restoreLatestSync() error {
	// Load all pins from the datastore.
	q := query.Query{
		Prefix: syncPrefix,
	}
	results, err := ing.ds.Query(context.Background(), q)
	if err != nil {
		return err
	}
	defer results.Close()

	var count int
	for r := range results.Next() {
		if r.Error != nil {
			return fmt.Errorf("cannot read latest syncs: %w", r.Error)
		}
		ent := r.Entry
		_, lastCid, err := cid.CidFromBytes(ent.Value)
		if err != nil {
			log.Errorw("Failed to decode latest sync CID", "err", err)
			continue
		}
		if lastCid == cid.Undef {
			continue
		}
		peerID, err := peer.Decode(strings.TrimPrefix(ent.Key, syncPrefix))
		if err != nil {
			log.Errorw("Failed to decode peer ID of latest sync", "err", err)
			continue
		}

		err = ing.sub.SetLatestSync(peerID, lastCid)
		if err != nil {
			log.Errorw("Failed to set latest sync", "err", err, "peer", peerID)
			continue
		}
		log.Debugw("Set latest sync", "provider", peerID, "cid", lastCid)
		count++
	}
	log.Infow("Loaded latest sync for providers", "count", count)
	return nil
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

// removeEntryAdMappings removes all existing temporary entry cid to ad cid
// mappings.  If the indexer terminated unexpectedly during a sync operation,
// then these map be left over and should be cleaned up on restart.
func removeEntryAdMappings(ctx context.Context, ds datastore.Batching) error {
	q := query.Query{
		Prefix: admapPrefix,
	}
	results, err := ds.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	var deletes []string
	for r := range results.Next() {
		if r.Error != nil {
			return err
		}
		ent := r.Entry
		deletes = append(deletes, ent.Key)
	}

	if len(deletes) != 0 {
		b, err := ds.Batch(ctx)
		if err != nil {
			return err
		}
		for i := range deletes {
			err = b.Delete(ctx, datastore.NewKey(deletes[i]))
			if err != nil {
				return err
			}
		}
		err = b.Commit(ctx)
		if err != nil {
			return err
		}

		log.Warnw("Cleaned up old temporary entry to ad mappings", "count", len(deletes))
	}
	return nil
}
