package ingest

// TODO
// 1. This should be its own package and accept an interrface that represents the ingester
// 2. How should callers handle errors?
// 3. How should callers clean up resources here?
// 4. Remove the other dead code.
// 5. Cleanup entries state in ingester data store. Can we namespace this and wipe the whole namespace? Do we want to?
// 6. More tests
// 7. Easy way for users of ingester to wait for a adv to be ingested.

// TODO namings
// s/adv/ad
// s/entriesChunk/entryChunk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// updateProcessor handles updating the indexer state for a given provider.
// When a sync is completed (by go-legs or otherwise), you should call `updateProcessor.queueUpdate`.
//
// Advertisements can be thought of as a state change. By applying an
// advertisement you take the state of the indexer from one older state to a
// newer state. The indexer is considered up to date for a given provider when
// it has applied every advertisement in order.
//
// An older advertisement is defined as being closer to the "genesis"
// advertisement, an advertisement that doesn't have a previous link. A newer
// advertisement is defined as an advertisement that is farther away from the
// genesis block via the previous links. You can equivalently think of this as a
// block height. Newer advertisements have a higher block height.
//
// The updateProcessor will apply each advertisement in order from oldest to
// newest. It will start processing the first advertisement after the last known
// processed advertisement. After applying an advertisement it will record that
// the advertisement has been processed.
type updateProcessor struct {
	publisher peer.ID
	// queueUpdate takes new heads and will schedule updating the indexer state to that new state
	queueUpdateChan chan cid.Cid

	nextHeadToProcess atomic.Value
	queueRun          chan struct{}

	subscribersRWMutex sync.RWMutex
	subscribers        []chan<- allAdvBelowHeadApplied

	appliedState  datastore.Batching
	ingesterStore datastore.Batching

	syncTimeout time.Duration

	syncFn              syncHelper
	indexContentBlock   indexContentBlockHelper
	signalMetricsUpdate func()
}

type syncHelper func(ctx context.Context, peerID peer.ID, c cid.Cid, sel ipld.Node) (cid.Cid, error)
type indexContentBlockHelper func(advCid cid.Cid, adv schema.Advertisement, provider peer.ID, entriesChunk ipld.Node) error

func newUpdateProcessor(provider peer.ID, ds datastore.Batching, syncFn syncHelper, indexContentBlock indexContentBlockHelper, signalMetricsUpdate func(), opts ...updateProcessorOpts) *updateProcessor {
	queueUpdateChan := make(chan cid.Cid, defaultUpdateProcessorUpdateChanBufferSize)

	c := &updateProcessorCfg{}
	for _, o := range opts {
		o(c)
	}

	var nextHeadToProcess atomic.Value
	nextHeadToProcess.Store(cid.Undef)
	return &updateProcessor{
		publisher:       provider,
		queueUpdateChan: queueUpdateChan,

		nextHeadToProcess: nextHeadToProcess,
		queueRun:          make(chan struct{}),

		appliedState:  namespace.Wrap(ds, datastore.NewKey(syncPrefix)),
		ingesterStore: ds,

		syncTimeout: c.syncTimeout,

		syncFn:              syncFn,
		indexContentBlock:   indexContentBlock,
		signalMetricsUpdate: signalMetricsUpdate,
	}
}

type updateProcessorCfg struct {
	syncTimeout time.Duration
}
type updateProcessorOpts func(*updateProcessorCfg)

func syncTimeoutForHydrate(d time.Duration) updateProcessorOpts {
	return func(c *updateProcessorCfg) {
		c.syncTimeout = d
	}
}

func (up *updateProcessor) close() {
	close(up.queueUpdateChan)
	close(up.queueRun)
}

type allAdvBelowHeadApplied struct {
	head cid.Cid
}

func (up *updateProcessor) onAllAdvApplied() (<-chan allAdvBelowHeadApplied, context.CancelFunc) {
	c := make(chan allAdvBelowHeadApplied, 1)
	up.subscribersRWMutex.Lock()
	defer up.subscribersRWMutex.Unlock()
	up.subscribers = append(up.subscribers, c)
	cncl := func() {
		up.subscribersRWMutex.Lock()
		defer up.subscribersRWMutex.Unlock()
		for i, ca := range up.subscribers {
			if ca == c {
				up.subscribers[i] = up.subscribers[len(up.subscribers)-1]
				up.subscribers[len(up.subscribers)-1] = nil
				up.subscribers = up.subscribers[:len(up.subscribers)-1]
				close(c)
				break
			}
		}
	}
	return c, cncl
}

func (up *updateProcessor) queueUpdate(newHead cid.Cid) {
	up.queueUpdateChan <- newHead
}

// pruneNextUpdates drops older head updates. Should run this from a separate goroutine.
func (up *updateProcessor) pruneNextUpdates() {
	for {
		nextHead, ok := <-up.queueUpdateChan
		if !ok {
			return
		}
		up.nextHeadToProcess.Store(nextHead)
		select {
		case up.queueRun <- struct{}{}:
		default: // Already queued
		}
	}
}

// advsUntilApplied returns a list of advCids until we reach an adv that we have seen.
// The leftmost element is the head.
func (up *updateProcessor) advsUntilApplied(head cid.Cid) ([]cid.Cid, error) {
	var lastMarked cid.Cid
	lastMarkedCidBytes, err := up.appliedState.Get(context.Background(), datastore.NewKey(up.publisher.String()))
	if err == datastore.ErrNotFound {
		lastMarked = cid.Undef
	} else if err != nil {
		return nil, err
	} else {
		_, lastMarked, err = cid.CidFromBytes(lastMarkedCidBytes)
		if err != nil {
			return nil, err
		}
	}

	if head == lastMarked {
		return nil, nil
	}

	adv, err := up.getAdv(head)
	if err != nil {
		return nil, err
	}

	var advChain []cid.Cid
	advChain = append(advChain, head)
	for {
		if adv.PreviousID.IsAbsent() {
			return advChain, nil
		}

		prevLink, err := adv.PreviousID.AsNode().AsLink()
		if err != nil {
			return nil, err
		}
		prevCid := prevLink.(cidlink.Link).Cid

		if prevCid == lastMarked {
			return advChain, nil
		}

		advChain = append(advChain, prevCid)

		adv, err = up.getAdv(prevCid)
		if err != nil {
			return nil, err
		}
	}
}

type entriesChunkIter struct {
	currentCid      cid.Cid
	isDone          bool
	getEntriesChunk func(cid.Cid) (schema.EntryChunk, error)
}

func newEntriesChunkIter(rootEntriesChunk cid.Cid, getEntriesChunk func(cid.Cid) (schema.EntryChunk, error)) *entriesChunkIter {
	return &entriesChunkIter{
		isDone:          rootEntriesChunk == cid.Undef,
		currentCid:      rootEntriesChunk,
		getEntriesChunk: getEntriesChunk,
	}
}

func (i *entriesChunkIter) done() bool {
	return i.isDone
}

func (i *entriesChunkIter) next() (schema.EntryChunk, error) {
	if i.isDone {
		return nil, nil
	}

	entriesChunk, err := i.getEntriesChunk(i.currentCid)
	if err != nil {
		return nil, err
	}

	if entriesChunk.Next.IsAbsent() {
		i.isDone = true
		i.currentCid = cid.Undef
	} else {
		nextLink, err := entriesChunk.Next.AsNode().AsLink()
		if err != nil {
			return nil, err
		}

		i.currentCid = nextLink.(cidlink.Link).Cid
	}

	return entriesChunk, nil
}

func (up *updateProcessor) applyAdv(advCid cid.Cid) error {
	adv, err := up.getAdv(advCid)
	if err != nil {
		return err
	}

	up.syncEntries(adv)
	entriesChunkLink, err := adv.Entries.AsLink()
	if err != nil {
		return err
	}
	entriesChunkCid := entriesChunkLink.(cidlink.Link).Cid

	for it := newEntriesChunkIter(entriesChunkCid, up.getEntriesChunk); !it.done(); {
		entriesChunk, err := it.next()
		if err != nil {
			return err
		}

		err = up.indexContentBlock(advCid, adv, up.publisher, entriesChunk)
	}

	return nil
}

func (up *updateProcessor) getAdv(advCid cid.Cid) (schema.Advertisement, error) {
	// Get data corresponding to the block.
	val, err := up.ingesterStore.Get(context.Background(), dsKey(advCid.String()))
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

	_, _, err = verifyAdvertisement(n)
	if err != nil {
		return nil, err
	}

	return adv, nil
}

func (up *updateProcessor) getEntriesChunk(entriesChunkCid cid.Cid) (schema.EntryChunk, error) {
	// Get data corresponding to the block.
	val, err := up.ingesterStore.Get(context.Background(), dsKey(entriesChunkCid.String()))
	if err != nil {
		log.Errorw("Error while fetching the node from datastore", "err", err)
		return nil, err
	}

	nb := schema.Type.EntryChunk.NewBuilder()
	err = dagjson.Decode(nb, bytes.NewReader(val))
	if err != nil {
		return nil, err
	}
	entriesChunk, ok := nb.Build().(schema.EntryChunk)
	if !ok {
		return nil, errors.New("type assertion failed for entries chunk")
	}

	return entriesChunk, nil
}

func (up *updateProcessor) syncEntries(adv schema.Advertisement) error {
	elink, err := adv.FieldEntries().AsLink()
	if err != nil {
		log.Errorw("Error decoding advertisement entries link", "err", err)
		return err
	}
	entriesCid := elink.(cidlink.Link).Cid

	ctx := context.Background()
	if up.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, up.syncTimeout)
		defer cancel()
	}
	startTime := time.Now()
	// Fully traverse the entries, because:
	//  * if the head is not persisted locally there is a chance we do not have it.
	//  * chain of entries as specified by EntryChunk schema only contain entries.
	_, err = up.syncFn(ctx, up.publisher, entriesCid, selectorparse.CommonSelector_ExploreAllRecursively)
	if err != nil {
		log.Errorw("Failed to sync", "err", err)
		return err
	}
	elapsed := time.Since(startTime)
	// Record how long sync took.
	stats.Record(context.Background(), metrics.SyncLatency.M(float64(elapsed.Nanoseconds())/1e6))
	log.Infow("Finished syncing entries", "elapsed", elapsed)

	return nil
}

func (up *updateProcessor) markAdvApplied(advCid cid.Cid) error {
	err := up.appliedState.Put(context.Background(), datastore.NewKey(up.publisher.String()), advCid.Bytes())
	if err != nil {
		log.Errorw("Error persisting latest sync", "err", err, "peer", up.publisher)
	}

	log.Debugw("Persisted latest sync", "peer", up.publisher, "cid", advCid)
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "libp2p2")),
		stats.WithMeasurements(metrics.IngestChange.M(1)))

	up.signalMetricsUpdate()
	return err
}

// run is the main run loop of the update processsor
func (up *updateProcessor) run() error {
	go up.pruneNextUpdates()
	for range up.queueRun {
		head := up.nextHeadToProcess.Load().(cid.Cid)
		fmt.Println("Processing head: ", head)

		// Get a list of advCids to apply
		advsToApply, err := up.advsUntilApplied(head)
		if err != nil {
			return err
		}
		fmt.Println("adv to apply: ", advsToApply)

		// Apply each adv, oldest to newest.
		for i := len(advsToApply) - 1; i >= 0; i-- {
			err = up.applyAdv(advsToApply[i])
			if err != nil {
				return err
			}

			// Mark the adv as applied
			err = up.markAdvApplied(advsToApply[i])
			if err != nil {
				return err
			}
		}

		// TODO cleanup (?)

		// Notify all subscribers that we have applied all advs until the head
		up.subscribersRWMutex.RLock()
		for _, ch := range up.subscribers {
			ch <- allAdvBelowHeadApplied{head: head}
		}
		up.subscribersRWMutex.RUnlock()

		fmt.Println("Finished processing advs", head)
	}
	return nil
}
