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

	go ing.metricsUpdater()

	log.Debugf("Ingester started and all hooks and linksystem registered")

	return ing, nil
}

func (ing *Ingester) Close() error {
	ing.adWaiter.close()
	// Close leg transport.
	err := ing.sub.Close()
	close(ing.sigUpdate)
	return err
}

// Sync syncs the latest advertisement from a publisher.  This is done by first
// fetching the latest advertisement ID from and traversing it until traversal
// gets to the last seen advertisement.  Then the entries in each advertisement
// are synced and the multihashes in each entry are indexed.
//
// The depth argument specifies the recursion depth limit to use during sync.
// When depth is set to 0, the advertisement recursion depth from config.Ingest
// used.
//
// Note that the reference to the latest synced advertisement returned by
// GetLatestSync is only updated if the given depth is zero.  Otherwise, a
// selector with the given depth limit is constructed  and used for traversal.
// The value of -1 signals no limit.
//
// The Context argument controls the lifetime of the sync.  Canceling it
// cancels the sync and causes the multihash channel to close without any data.
//
// Note that the multihash entries corresponding to the advertisement are
// synced in the background.  The completion of advertisement sync does not
// necessarily mean that the entries corresponding to the advertisement are
// synced.
func (ing *Ingester) Sync(ctx context.Context, peerID peer.ID, peerAddr multiaddr.Multiaddr, depth int64) (<-chan multihash.Multihash, error) {
	log := log.With("peerID", peerID)
	log.Debug("Explicitly syncing the latest advertisement from peer")

	out := make(chan multihash.Multihash, 1)
	go func() {
		defer close(out)

		var sel ipld.Node
		// If depth is non-zero, construct a selector for sync accordingly.
		if depth != 0 {
			log.With("depth", depth)
			if depth < -1 {
				log.Error("Recursion depth limit must not be less than -1")
				return
			}

			// Consider the value of -1 as no-limit, similar to config.Ingest.
			var rLimit selector.RecursionLimit
			if depth == -1 {
				rLimit = selector.RecursionLimitNone()
			} else {
				rLimit = selector.RecursionLimitDepth(depth)
			}

			latest, err := ing.GetLatestSync(peerID)
			if err != nil {
				log.Errorw("Failed to get the latest synced while constructing selector", "err", err)
				return
			}

			var stopAt ipld.Link
			if latest != cid.Undef {
				stopAt = cidlink.Link{Cid: latest}
			}
			log = log.With("stopAt", stopAt)

			ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
			adSequence := ssb.ExploreFields(
				func(efsb builder.ExploreFieldsSpecBuilder) {
					efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
				}).Node()

			sel = legs.ExploreRecursiveWithStopNode(rLimit, adSequence, stopAt)
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
			log.Errorw("Failed to sync with provider", "err", err, "provider", peerID)
			return
		}
		// Do not persist the latest sync here, because that is done in
		// after we've processed the ad.

		ing.signalMetricsUpdate()
		// Notification channel; buffered so as not to block if no reader.
		out <- c.Hash()
	}()

	return out, nil
}

func (ing *Ingester) markAdProcessed(publisher peer.ID, adCid cid.Cid) error {
	log.Debugw("Persisted latest sync", "peer", publisher, "cid", adCid)
	return ing.ds.Put(context.Background(), datastore.NewKey(syncPrefix+publisher.String()), adCid.Bytes())
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
