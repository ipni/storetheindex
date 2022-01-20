package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	indexerengine "github.com/filecoin-project/go-indexer-core/engine"
	coremetrics "github.com/filecoin-project/go-indexer-core/metrics"
	"github.com/filecoin-project/go-legs"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
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
	"github.com/multiformats/go-varint"
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

// Ingester is a type that uses go-legs for the ingestion protocol.
type Ingester struct {
	host    host.Host
	ds      datastore.Batching
	indexer *indexerengine.Engine

	batchSize int
	sigUpdate chan struct{}

	sub           *legs.Subscriber
	cancelSyncFin context.CancelFunc
	syncTimeout   time.Duration
	watchDone     chan struct{}

	// Processors that apply ad chains to the indexer.
	adchainprocessors    map[peer.ID]*adchainprocessor.Processor
	adchainprocessorLock sync.Mutex

	adchainprocessorErrors     map[peer.ID][]error
	adchainprocessorErrorsLock sync.Mutex
}

// NewIngester creates a new Ingester that uses a go-legs Subscriber to handle
// communication with providers.
func NewIngester(cfg config.Ingest, h host.Host, idxr *indexerengine.Engine, reg *registry.Registry, ds datastore.Batching) (*Ingester, error) {
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
	ing.adchainprocessorLock.Lock()
	for _, p := range ing.adchainprocessors {
		p.Close()
	}
	ing.adchainprocessors = nil
	ing.adchainprocessorLock.Unlock()

	ing.cancelSyncFin()
	<-ing.watchDone

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

func (ing *Ingester) GetProcessedUpTo(ctx context.Context, peerID peer.ID) (cid.Cid, uint, error) {
	b, err := ing.ds.Get(ctx, datastore.NewKey(processedPrefix+peerID.String()))

	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, 0, nil
		}
		return cid.Undef, 0, err
	}

	blockHeight, bytesRead, err := varint.FromUvarint(b)
	if err != nil {
		return cid.Undef, 0, err
	}

	_, c, err := cid.CidFromBytes(b[bytesRead:])
	return c, uint(blockHeight), err
}

func (ing *Ingester) PutProcessedUpTo(ctx context.Context, peerID peer.ID, c cid.Cid, blockHeight uint) error {
	n := varint.ToUvarint(uint64(blockHeight))
	out := append(n, c.Bytes()...)

	err := ing.ds.Put(ctx, datastore.NewKey(processedPrefix+peerID.String()), out)

	log.Debugw("Persisted latest processed", "peer", peerID, "cid", c)
	_ = stats.RecordWithOptions(ctx,
		stats.WithTags(tag.Insert(metrics.Method, "libp2p2")),
		stats.WithMeasurements(metrics.IngestChange.M(1)))

	ing.signalMetricsUpdate()
	return err
}

func (ing *Ingester) SyncDag(ctx context.Context, peerID peer.ID, c cid.Cid, sel ipld.Node) (cid.Cid, error) {
	return ing.sub.Sync(ctx, peerID, c, sel, nil)
}

// IndexContentBlock indexes the content CIDs in a block of data.  First the
// advertisement is loaded to get the context ID and metadata, and then that
// and the CIDs in the content block are indexed by the indexer-core.
//
// The pubID is the peer ID of the message publisher.  This is not necessarily
// the same as the provider ID in the advertisement.  The publisher is the
// source of the indexed content, the provider is where content can be
// retrieved from.  It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) IndexContentBlock(adCid cid.Cid, ad schema.Advertisement, pubID peer.ID, nentries ipld.Node) error {
	// Decode the list of cids into a List_String
	nb := schema.Type.EntryChunk.NewBuilder()
	err := nb.AssignNode(nentries)
	if err != nil {
		return fmt.Errorf("cannot decode entries: %s", err)
	}

	nchunk := nb.Build().(schema.EntryChunk)

	// Load the advertisement data for this chunk.
	value, isRm, err := ing.loadAdData(ad)
	if err != nil {
		return err
	}

	mhChan := make(chan multihash.Multihash, ing.batchSize)
	// The isRm parameter is passed in for an advertisement that contains
	// entries, to allow for removal of individual entries.
	errChan := ing.batchIndexerEntries(mhChan, value, isRm)

	// Iterate over all entries and ingest (or remove) them.
	entries := nchunk.FieldEntries()
	cit := entries.ListIterator()
	var count int
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		if err != nil {
			close(mhChan)
			return fmt.Errorf("cannot decode an entry from the ingestion list: %s", err)
		}

		select {
		case mhChan <- h:
		case err = <-errChan:
			return err
		}

		count++
	}
	close(mhChan)
	err = <-errChan
	if err != nil {
		if isRm {
			return fmt.Errorf("cannot remove multihashes from indexer: %s", err)
		}
		return fmt.Errorf("cannot put multihashes into indexer: %s", err)
	}

	return nil
}

func (ing *Ingester) loadAdData(ad schema.Advertisement) (indexer.Value, bool, error) {
	// Fetch data of interest.
	contextID, err := ad.FieldContextID().AsBytes()
	if err != nil {
		return indexer.Value{}, false, err
	}
	metadataBytes, err := ad.FieldMetadata().AsBytes()
	if err != nil {
		return indexer.Value{}, false, err
	}
	isRm, err := ad.FieldIsRm().AsBool()
	if err != nil {
		return indexer.Value{}, false, err
	}

	// The peerID passed into the storage hook is the source of the
	// advertisement (the publisher), and not necessarily the same as the
	// provider in the advertisement.  Read the provider from the advertisement
	// to create the indexed value.
	providerID, err := providerFromAd(ad)
	if err != nil {
		return indexer.Value{}, false, err
	}

	// Check for valid metadata
	err = new(v0.Metadata).UnmarshalBinary(metadataBytes)
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decoding metadata: %s", err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadataBytes,
	}

	return value, isRm, nil
}

// batchIndexerEntries starts a goroutine that processes batches of multihashes
// from an input channels.  The goroutine collects these into a slice, storing
// up to batchSize elements.  When the slice is at capacity, a Put or Remove
// request is made to the indexer core depending on the whether isRm is true
// or false.  This function returns an error channel that returns an error if
// one occurs during processing.  This also indicates the goroutine has exited
// (and will no longer read its input channel).
//
// The goroutine exits when the input channel is closed.  It closes the error
// channel to indicate completion.
func (ing *Ingester) batchIndexerEntries(mhChan <-chan multihash.Multihash, value indexer.Value, isRm bool) <-chan error {
	var indexFunc func(indexer.Value, ...multihash.Multihash) error
	var opName string
	if isRm {
		indexFunc = ing.indexer.Remove
		opName = "remove"
	} else {
		indexFunc = ing.indexer.Put
		opName = "put"
	}

	errChan := make(chan error, 1)

	go func(batchSize int) {
		defer close(errChan)
		batch := make([]multihash.Multihash, 0, batchSize)
		var count int
		for m := range mhChan {
			batch = append(batch, m)
			if len(batch) == batchSize {
				// Process full batch of multihashes
				if err := indexFunc(value, batch...); err != nil {
					errChan <- err
					return
				}
				batch = batch[:0]
				count += batchSize
			}
		}

		if len(batch) != 0 {
			// Process any remaining puts
			if err := indexFunc(value, batch...); err != nil {
				errChan <- err
				return
			}
			count += len(batch)
		}

		log.Infow("Processed multihashes in entry chunk", "count", count, "operation", opName)
	}(ing.batchSize)

	return errChan
}

func (ing *Ingester) processAdChain(publisher peer.ID, head cid.Cid) {
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

	ing.adchainprocessorLock.Lock()
	p, ok := ing.adchainprocessors[provID]
	if !ok {
		p = adchainprocessor.NewUpdateProcessor(provID, ing, adchainprocessor.Publisher(publisher))
		ing.adchainprocessors[provID] = p
	}
	ing.adchainprocessorLock.Unlock()

	go func() {
		log.Debug("Starting update processor for", head)
		err := p.Run(context.Background(), head)
		if err != nil {
			log.Errorf("Failed to run update processor for peer %s: %v", provID, err)
			ing.adchainprocessorErrorsLock.Lock()
			defer ing.adchainprocessorErrorsLock.Unlock()
			errs := ing.adchainprocessorErrors[provID]
			errs = append(errs, err)
			ing.adchainprocessorErrors[provID] = errs
		}
	}()
}

// watchSyncFinished reads legs.SyncFinished events and records the latest sync
// for the peer that was synced.
func (ing *Ingester) watchSyncFinished(onSyncFin <-chan legs.SyncFinished) {
	for syncFin := range onSyncFin {
		ing.processAdChain(syncFin.PeerID, syncFin.Cid)

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
