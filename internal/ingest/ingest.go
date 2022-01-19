package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core/engine"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/ingest/adchainprocessor"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer/ingest")

// prefix used to track latest sync in datastore.
const (
	syncPrefix      = "/sync/"
	processedPrefix = "/proc/"
	admapPrefix     = "/admap/"
)

const delayAfterAdChainProcessingError = 5 * time.Second

// Ingester is a type that uses go-legs for the ingestion protocol.
type Ingester struct {
	host    host.Host
	ds      datastore.Batching
	indexer *indexer.Engine

	batchSize int
	sigUpdate chan struct{}

	sub           *legs.Subscriber
	cancelSyncFin context.CancelFunc
	syncTimeout   time.Duration
	adLocks       *lockChain
	watchDone     chan struct{}

	// Processors that apply ad chains to the indexer.
	adchainprocessors    map[peer.ID]*adchainprocessor.Processor
	adchainprocessorLock sync.RWMutex

	adchainprocessorErrors     map[peer.ID][]error
	adchainprocessorErrorsLock sync.Mutex
}

// NewIngester creates a new Ingester that uses a go-legs Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr *indexer.Engine, reg *registry.Registry, ds datastore.Batching) (*Ingester, error) {
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

	ing := &Ingester{
		host:                   h,
		ds:                     ds,
		indexer:                idxr,
		batchSize:              cfg.StoreBatchSize,
		sigUpdate:              make(chan struct{}, 1),
		syncTimeout:            time.Duration(cfg.SyncTimeout),
		adLocks:                newLockChain(),
		watchDone:              make(chan struct{}),
		adchainprocessors:      make(map[peer.ID]*adchainprocessor.Processor),
		adchainprocessorErrors: make(map[peer.ID][]error),
	}

	// Create and start pubsub subscriber.  This also registers the storage
	// hook to index data as it is received.
	sub, err := legs.NewSubscriber(h, ds, lsys, cfg.PubSubTopic, adSel, legs.AllowPeer(reg.Authorized))
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

	onSyncFin, cancelSyncFin := sub.OnSyncFinished()
	ing.cancelSyncFin = cancelSyncFin

	go ing.watchSyncFinished(onSyncFin)
	go ing.metricsUpdater()

	log.Debugf("Ingester started and all hooks and linksystem registered")

	return ing, nil
}

func (ing *Ingester) Close() error {
	ing.cancelSyncFin()
	<-ing.watchDone

	ing.adchainprocessorLock.Lock()
	for _, p := range ing.adchainprocessors {
		p.Close()
	}
	ing.adchainprocessors = make(map[peer.ID]*adchainprocessor.Processor)
	ing.adchainprocessorLock.Unlock()

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
// The Context argument controls the lifetime of the sync.  Canceling it
// cancels the sync and causes the multihash channel to close without any data.
//
// Note that the multihash entries corresponding to the advertisement are
// synced in the background.  The completion of advertisement sync does not
// necessarily mean that the entries corresponding to the advertisement are
// synced.
func (ing *Ingester) Sync(ctx context.Context, peerID peer.ID, peerAddr multiaddr.Multiaddr) (<-chan multihash.Multihash, error) {
	log := log.With("peerID", peerID)
	log.Debug("Explicitly syncing the latest advertisement from peer")

	out := make(chan multihash.Multihash, 1)
	go func() {
		defer close(out)

		// Start syncing. Notifications for the finished sync are sent
		// asynchronously.  Sync with cid.Undef and a nil selector so that:
		//
		//   1. The latest head is queried by go-legs via head-publisher.
		//
		//   2. The default selector is used where traversal stops at the
		//      latest known head.
		c, err := ing.sub.Sync(ctx, peerID, cid.Undef, nil, peerAddr)
		if err != nil {
			log.Errorw("Failed to sync with provider", "err", err, "provider", peerID)
			return
		}
		// Do not persist the latest sync here, because that is done in
		// watchSyncFinished.

		// Notification channel; buffered so as not to block if no reader.
		out <- c.Hash()
		ing.signalMetricsUpdate()
	}()

	return out, nil
}

func (ing *Ingester) GetAd(ctx context.Context, adCid cid.Cid) (schema.Advertisement, error) {
	// Get data corresponding to the block.
	val, err := ing.GetBlock(ctx, adCid)
	if err != nil {
		log.Errorw("Error while fetching the node from datastore", "err", err)
		return nil, err
	}

	nb := schema.Type.Advertisement.NewBuilder()
	err = dagjson.Decode(nb, bytes.NewReader(val))
	if err != nil {
		return nil, err
	}
	n := nb.Build()
	adv, ok := n.(schema.Advertisement)
	if !ok {
		return nil, errors.New("type assertion failed for advertisement")
	}

	return adv, nil
}

func (ing *Ingester) GetBlock(ctx context.Context, c cid.Cid) ([]byte, error) {
	return ing.ds.Get(ctx, dsKey(c.String()))
}

func (ing *Ingester) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return ing.ds.Delete(ctx, dsKey(c.String()))
}

func (ing *Ingester) GetProcessedUpTo(ctx context.Context, peerID peer.ID) (cid.Cid, error) {
	b, err := ing.ds.Get(ctx, datastore.NewKey(processedPrefix+peerID.String()))

	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

func (ing *Ingester) PutProcessedUpTo(ctx context.Context, peerID peer.ID, c cid.Cid) error {
	return ing.ds.Put(ctx, datastore.NewKey(processedPrefix+peerID.String()), c.Bytes())
}

func (ing *Ingester) SyncDag(ctx context.Context, peerID peer.ID, c cid.Cid, sel ipld.Node) (cid.Cid, error) {
	return ing.sub.Sync(ctx, peerID, c, sel, nil)
}

func (ing *Ingester) waitForAdvProcessed(ctx context.Context, peerID peer.ID, c cid.Cid) error {
	ing.adchainprocessorLock.RLock()
	p, ok := ing.adchainprocessors[peerID]
	ing.adchainprocessorLock.RUnlock()
	if !ok {
		return errors.New("missing processor for peer")
	}

	advAppliedChan, cncl := p.OnAllAdApplied()
	defer cncl()
	for {
		select {
		case <-ctx.Done():
			return errors.New("context cancelled while waiting for adv to be processed")
		case appliedAdCid := <-advAppliedChan:
			if appliedAdCid.Head == c {
				return nil
			}
		}
	}
}

func (ing *Ingester) queueAdChainProcessor(publisher peer.ID, head cid.Cid) {
	ad, err := ing.GetAd(context.Background(), head)
	if err != nil {
		log.Warnf("failed to get advertisement for head %s: %s", head, err)
		return
	}

	provID, err := peer.Decode(ad.Provider.String())
	if err != nil {
		log.Errorf("failed to get parse provider id for head %s: %s", head, err)
		return
	}

	ing.adchainprocessorLock.RLock()
	p, ok := ing.adchainprocessors[provID]
	ing.adchainprocessorLock.RUnlock()
	log.Debug("Starting update processor for", head)
	if !ok {
		p = adchainprocessor.NewUpdateProcessor(provID, ing, ing.signalMetricsUpdate, adchainprocessor.Publisher(publisher))
		ing.adchainprocessorLock.Lock()
		ing.adchainprocessors[provID] = p
		ing.adchainprocessorLock.Unlock()
		go func() {
			for {
				err := p.Run()
				if err != nil {
					log.Errorf("Failed to run update processor for peer %s: %s", provID, err)

					ing.adchainprocessorErrorsLock.Lock()
					errs := ing.adchainprocessorErrors[provID]
					errs = append(errs, err)
					ing.adchainprocessorErrors[provID] = errs
					ing.adchainprocessorErrorsLock.Unlock()
				}
				time.Sleep(delayAfterAdChainProcessingError)
			}
		}()
	}

	p.QueueUpdate(head)
}

// watchSyncFinished reads legs.SyncFinished events and records the latest sync
// for the peer that was synced.
func (ing *Ingester) watchSyncFinished(onSyncFin <-chan legs.SyncFinished) {
	for syncFin := range onSyncFin {
		ing.queueAdChainProcessor(syncFin.PeerID, syncFin.Cid)

		// Persist the latest sync
		err := ing.ds.Put(context.Background(), datastore.NewKey(syncPrefix+syncFin.PeerID.String()), syncFin.Cid.Bytes())
		if err != nil {
			log.Errorw("Error persisting latest sync", "err", err, "peer", syncFin.PeerID)
			continue
		}
		log.Debugw("Persisted latest sync", "peer", syncFin.PeerID, "cid", syncFin.Cid)
		_ = stats.RecordWithOptions(context.Background(),
			stats.WithTags(tag.Insert(metrics.Method, "libp2p2")),
			stats.WithMeasurements(metrics.IngestChange.M(1)))

		ing.signalMetricsUpdate()
	}
	close(ing.watchDone)
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
	var hasUpdate bool
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
func (ing *Ingester) getLatestSync(peerID peer.ID) (cid.Cid, error) {
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
