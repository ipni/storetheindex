package adchainprocessor

// TODO
// 4. Remove the other dead code.

// Open Qs:
//
// BUG(marco) What happens if we get a forked chain. We originally see A1 A2 A3,
// but then get A1' A2' A3'. What is the correct behavior?
//
// BUG(marco) Should we keep a track of which block has been applied? Could to
// know if we've forked in the case of A1 A2 A3, then A1' A2'.

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.opencensus.io/stats"
)

var log = logging.Logger("indexer/ingest/adchainprocessor")

var errAlreadyApplied = errors.New("already applied head")

// Processor handles updating the indexer state for a given provider.
// When a sync is completed (by go-legs or otherwise), you should call `Processor.Run`.
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
// The Processor will apply each advertisement in order from oldest to
// newest. It will start processing the first advertisement after the last known
// processed advertisement. After applying an advertisement it will record that
// the advertisement has been processed.
type Processor struct {
	running sync.Mutex
	closed  bool

	publisher peer.ID
	provider  peer.ID

	subscribersRWMutex sync.RWMutex
	subscribers        []chan<- AllAdBelowHeadApplied

	syncTimeout time.Duration

	ingester Ingester
}

type Ingester interface {
	GetAd(context.Context, cid.Cid) (schema.Advertisement, error)
	GetBlock(context.Context, cid.Cid) ([]byte, error)
	DeleteBlock(context.Context, cid.Cid) error
	GetProcessedUpTo(ctx context.Context, provider peer.ID) (applied cid.Cid, blockHeight uint, err error)
	PutProcessedUpTo(ctx context.Context, provider peer.ID, applied cid.Cid, blockHeight uint) error
	SyncDag(ctx context.Context, provider peer.ID, c cid.Cid, sel ipld.Node) (cid.Cid, error)
	IndexContentBlock(adCid cid.Cid, ad schema.Advertisement, provider peer.ID, entryChunk ipld.Node) error
}

func NewUpdateProcessor(provider peer.ID, ingester Ingester, opts ...updateProcessorOpts) *Processor {
	c := &updateProcessorCfg{}
	for _, o := range opts {
		o(c)
	}

	return &Processor{
		provider:  provider,
		publisher: c.publisher,

		// TODO this can be handled by the ingester when providing Sync
		syncTimeout: c.syncTimeout,

		ingester: ingester,
	}
}

type updateProcessorCfg struct {
	publisher   peer.ID
	syncTimeout time.Duration
}
type updateProcessorOpts func(*updateProcessorCfg)

func SyncTimeoutForHydrate(d time.Duration) updateProcessorOpts {
	return func(c *updateProcessorCfg) {
		c.syncTimeout = d
	}
}

// Publisher sets a different publisher than the provider. By default the provider is the publisher.
func Publisher(publisher peer.ID) updateProcessorOpts {
	return func(c *updateProcessorCfg) {
		c.publisher = publisher
	}
}

func (p *Processor) Close() {
	p.subscribersRWMutex.Lock()
	defer p.subscribersRWMutex.Unlock()
	for _, ch := range p.subscribers {
		close(ch)
	}
	p.subscribers = nil

	p.running.Lock()
	p.closed = true
	defer p.running.Unlock()
}

type AllAdBelowHeadApplied struct {
	Head cid.Cid
}

func (p *Processor) OnAllAdApplied() (<-chan AllAdBelowHeadApplied, context.CancelFunc) {
	c := make(chan AllAdBelowHeadApplied, 1)
	p.subscribersRWMutex.Lock()
	defer p.subscribersRWMutex.Unlock()
	p.subscribers = append(p.subscribers, c)
	cncl := func() {
		p.subscribersRWMutex.Lock()
		defer p.subscribersRWMutex.Unlock()
		for i, ca := range p.subscribers {
			if ca == c {
				p.subscribers[i] = p.subscribers[len(p.subscribers)-1]
				p.subscribers[len(p.subscribers)-1] = nil
				p.subscribers = p.subscribers[:len(p.subscribers)-1]
				close(c)
				break
			}
		}
	}
	return c, cncl
}

// adsUntilApplied returns a list of adCids until we reach an ad that we have seen.
// The leftmost element is the head. And returns the blockheight of the head cid.
func (p *Processor) adsUntilApplied(ctx context.Context, head cid.Cid) ([]cid.Cid, uint, error) {
	lastMarked, lastBlockHeight, err := p.ingester.GetProcessedUpTo(ctx, p.provider)
	if err != nil {
		return nil, 0, err
	}

	if head == lastMarked {
		return nil, 0, nil
	}

	ad, err := p.ingester.GetAd(ctx, head)
	if err != nil {
		return nil, 0, err
	}

	var adChain []cid.Cid
	adChain = append(adChain, head)
	for {
		if ad.PreviousID.IsAbsent() {
			// We're at the genesis block. Is this chain longer than our block height?
			if lastBlockHeight == 0 {
				// We haven't seen anything before, so nothing to compare against
				return adChain, uint(len(adChain)), nil
			} else if len(adChain) < int(lastBlockHeight) {
				// This chain is shorter than our block height. We've probably already applied this update.
				return nil, 0, errAlreadyApplied
			} else {
				// This chain is equal to or longer than our block height, but we haven't run into any blocks we've seen before.
				return nil, 0, errors.New("unexpected ad chain. head points to chain we haven't seen before")
			}
		}

		prevLink, err := ad.PreviousID.AsNode().AsLink()
		if err != nil {
			return nil, 0, err
		}
		prevCid := prevLink.(cidlink.Link).Cid

		if prevCid == lastMarked {
			return adChain, uint(len(adChain)) + lastBlockHeight, nil
		}

		adChain = append(adChain, prevCid)

		ad, err = p.ingester.GetAd(ctx, prevCid)
		if err != nil {
			return nil, 0, err
		}
	}
}

type entryChunkIter struct {
	currentCid    cid.Cid
	isDone        bool
	getEntryChunk func(context.Context, cid.Cid) (schema.EntryChunk, error)
}

func newentryChunkIter(rootentryChunk cid.Cid, getEntryChunk func(context.Context, cid.Cid) (schema.EntryChunk, error)) *entryChunkIter {
	return &entryChunkIter{
		isDone:        rootentryChunk == cid.Undef,
		currentCid:    rootentryChunk,
		getEntryChunk: getEntryChunk,
	}
}

func (i *entryChunkIter) done() bool {
	return i.isDone
}

func (i *entryChunkIter) next(ctx context.Context) (cid.Cid, schema.EntryChunk, error) {
	if i.isDone {
		return cid.Undef, nil, nil
	}

	entryChunk, err := i.getEntryChunk(ctx, i.currentCid)
	if err != nil {
		return cid.Undef, nil, err
	}
	entryChunkCid := i.currentCid

	if entryChunk.Next.IsAbsent() {
		i.isDone = true
		i.currentCid = cid.Undef
	} else {
		nextLink, err := entryChunk.Next.AsNode().AsLink()
		if err != nil {
			return cid.Undef, nil, err
		}

		i.currentCid = nextLink.(cidlink.Link).Cid
	}

	return entryChunkCid, entryChunk, nil
}

func (p *Processor) applyAd(ctx context.Context, adCid cid.Cid) error {
	ad, err := p.ingester.GetAd(ctx, adCid)
	if err != nil {
		return err
	}

	p.syncEntries(ctx, ad)
	entryChunkLink, err := ad.Entries.AsLink()
	if err != nil {
		return err
	}
	entryChunkCid := entryChunkLink.(cidlink.Link).Cid

	for it := newentryChunkIter(entryChunkCid, p.getEntryChunk); !it.done(); {
		entryChunkCid, entryChunk, err := it.next(ctx)
		if err != nil {
			return err
		}

		err = p.ingester.IndexContentBlock(adCid, ad, p.publisher, entryChunk)
		if err != nil {
			return err
		}

		// Remove the content block after indexing it. We may have to fetch it again
		// if another ad references it, but most of the time we won't.
		err = p.ingester.DeleteBlock(ctx, entryChunkCid)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Processor) getEntryChunk(ctx context.Context, entryChunkCid cid.Cid) (schema.EntryChunk, error) {
	// Get data corresponding to the block.
	val, err := p.ingester.GetBlock(ctx, entryChunkCid)
	if err != nil {
		log.Errorw("Error while fetching the node from datastore", "err", err, "cid", entryChunkCid)
		return nil, err
	}

	nb := schema.Type.EntryChunk.NewBuilder()
	err = dagjson.Decode(nb, bytes.NewReader(val))
	if err != nil {
		return nil, err
	}
	entryChunk, ok := nb.Build().(schema.EntryChunk)
	if !ok {
		return nil, errors.New("type assertion failed for entries chunk")
	}

	return entryChunk, nil
}

func (p *Processor) syncEntries(ctx context.Context, ad schema.Advertisement) error {
	elink, err := ad.FieldEntries().AsLink()
	if err != nil {
		log.Errorw("Error decoding advertisement entries link", "err", err)
		return err
	}
	entriesCid := elink.(cidlink.Link).Cid

	if p.syncTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.syncTimeout)
		defer cancel()
	}
	startTime := time.Now()
	// Fully traverse the entries, because:
	//  * if the head is not persisted locally there is a chance we do not have it.
	//  * chain of entries as specified by EntryChunk schema only contain entries.
	_, err = p.ingester.SyncDag(ctx, p.publisher, entriesCid, selectorparse.CommonSelector_ExploreAllRecursively)
	if err != nil {
		log.Errorw("Failed to sync", "err", err)
		return err
	}
	elapsed := time.Since(startTime)
	// Record how long sync took.
	stats.Record(ctx, metrics.SyncLatency.M(float64(elapsed.Nanoseconds())/1e6))
	log.Infow("Finished syncing entries", "elapsed", elapsed)

	return nil
}

// Run is the main Run loop of the update processsor. Will apply each block of
// the adchain up to and including the head. If it realizes it has already
// applied the head, it noops.
func (p *Processor) Run(ctx context.Context, headAdCid cid.Cid) error {
	// This has to happen serially since we apply blocks serially.
	p.running.Lock()
	defer p.running.Unlock()

	if p.closed {
		return nil
	}

	log.Debug("Processing ads to: ", headAdCid)

	// Get a list of adCids to apply
	adsToApply, newBlockHeight, err := p.adsUntilApplied(ctx, headAdCid)
	if err == errAlreadyApplied {
		log.Info("Aleady applied headAdCid: ", headAdCid)
		return nil
	} else if err != nil {
		return err
	}

	// TODO check if the current applied block height is greater than this headAdCid.

	// Apply each ad, oldest to newest.
	for i := len(adsToApply) - 1; i >= 0; i-- {
		err = p.applyAd(ctx, adsToApply[i])
		if err != nil {
			return err
		}

		// Mark the ad as applied
		err = p.ingester.PutProcessedUpTo(ctx, p.provider, adsToApply[i], newBlockHeight-uint(i))
		if err != nil {
			return err
		}
	}

	// Notify all subscribers that we have applied all ads up to and including the headAdCid
	p.subscribersRWMutex.RLock()
	for _, ch := range p.subscribers {
		ch <- AllAdBelowHeadApplied{Head: headAdCid}
	}
	p.subscribersRWMutex.RUnlock()

	log.Debug("Finished processing ads", headAdCid)

	return nil
}
