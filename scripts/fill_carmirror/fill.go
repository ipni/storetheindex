package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

const (
	stopGenesis  = "genesis"
	stopDepth    = "depth"
	stopCanceled = "user cancelled"
	stopError    = "error"
)

type source int

const (
	sourceNone source = iota
	sourceMain
	sourceExternal
	sourceProvider
)

func (s source) String() string {
	switch s {
	case sourceMain:
		return "main"
	case sourceExternal:
		return "external"
	case sourceProvider:
		return "provider"
	default:
		return "none"
	}
}

// Options configures a fill run. Mirror must have MainMode readwrite.
type Options struct {
	Mirror            config.Mirror
	HttpTimeout       time.Duration
	HttpRetryMax      int
	HttpRetryWaitMin  time.Duration
	HttpRetryWaitMax  time.Duration
	EntriesDepthLimit int64

	Provider  peer.ID
	StartAd   cid.Cid
	Publisher peer.AddrInfo
	Depth     int // 0 means unlimited

	// Estimate, when true, counts advertisements in the chain (ads only)
	// before filling so progress can show a total. EstimateTimeout limits
	// that count; 0 means no time limit. A timeout or error yields a partial
	// total and fill still runs.
	Estimate        bool
	EstimateTimeout time.Duration

	// Concurrency is the size of the publisher subscriber pool and the
	// maximum number of publisher rebuilds in flight. 0 means
	// defaultConcurrency.
	Concurrency int

	// Out receives fill events. Nil is a no-op.
	Out Observer
}

type carData struct {
	ad      schema.Advertisement
	adData  []byte
	entries []carstore.EntryBlock
	hamt    bool
	chunks  int
	mhs     int
	size    int64
}

type filler struct {
	opts       Options
	out        Observer
	ds         datastore.Batching
	mainReader *carstore.CarReader
	mainWriter *carstore.CarWriter
	externals  []*carstore.CarReader
	downloaded atomic.Int64

	host host.Host
	subs chan *pooledSub

	errMu sync.Mutex
	err   error
}

const defaultConcurrency = 8

// pooledSub is a reusable dagsync subscriber. onPut is swapped per borrow so
// entry-chunk progress is attributed to the advertisement currently using it.
type pooledSub struct {
	sub   *dagsync.Subscriber
	mu    sync.Mutex
	onPut func(cid.Cid, []byte)
}

func (p *pooledSub) setOnPut(fn func(cid.Cid, []byte)) {
	p.mu.Lock()
	p.onPut = fn
	p.mu.Unlock()
}

func (p *pooledSub) put(c cid.Cid, data []byte) {
	p.mu.Lock()
	fn := p.onPut
	p.mu.Unlock()
	if fn != nil {
		fn(c, data)
	}
}

// Fill walks a provider's advertisement chain and writes missing or invalid
// CAR files to the main advertisement mirror. Removal (IsRm) and no-entries
// ads are not stored. An IsRm ad is reported (whether a CAR already exists on
// main) and otherwise left untouched. It never opens the indexer value store.
func Fill(ctx context.Context, opts Options) error {
	f, err := newFiller(opts)
	if err != nil {
		observerOrNop(opts.Out).Done(stopError, err)
		return err
	}
	defer f.close()
	return f.run(ctx)
}

func newFiller(opts Options) (*filler, error) {
	opts.Mirror.PopulateUnset()
	if !opts.Mirror.MainMode.CanRead() || !opts.Mirror.MainMode.CanWrite() {
		return nil, fmt.Errorf("main car mirror must be readwrite (got MainMode %q)", opts.Mirror.MainMode)
	}
	if opts.Mirror.Main.Type == "" || opts.Mirror.Main.Type == "none" {
		return nil, errors.New("main car mirror has no storage backend")
	}

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	f := &filler{
		opts: opts,
		out:  observerOrNop(opts.Out),
		ds:   ds,
	}

	mainStore, err := filestore.MakeFilestore(opts.Mirror.Main.Config)
	if err != nil {
		return nil, fmt.Errorf("cannot create main car file store: %w", err)
	}
	if mainStore == nil {
		return nil, errors.New("main car mirror storage backend is disabled")
	}

	mainWriter, err := carstore.NewWriter(ds, mainStore, carstore.WithCompress(opts.Mirror.Main.Compress))
	if err != nil {
		return nil, fmt.Errorf("cannot create main car writer: %w", err)
	}
	mainReader, err := carstore.NewReader(mainStore, carstore.WithCompress(opts.Mirror.Main.Compress))
	if err != nil {
		return nil, fmt.Errorf("cannot create main car reader: %w", err)
	}

	var externals []*carstore.CarReader
	for i, ext := range opts.Mirror.External {
		if ext.Type == "" || ext.Type == "none" {
			continue
		}
		extStore, err := filestore.MakeFilestore(ext.Config)
		if err != nil {
			return nil, fmt.Errorf("cannot create external[%d] car store: %w", i, err)
		}
		if extStore == nil {
			continue
		}
		reader, err := carstore.NewReader(extStore, carstore.WithCompress(ext.Compress))
		if err != nil {
			return nil, fmt.Errorf("cannot create external[%d] car reader: %w", i, err)
		}
		externals = append(externals, reader)
	}

	f.mainReader = mainReader
	f.mainWriter = mainWriter
	f.externals = externals

	if err := f.initHost(); err != nil {
		return nil, err
	}
	if err := f.initSubPool(); err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}

func (f *filler) close() {
	f.closeSubs()
	if f.host != nil {
		_ = f.host.Close()
		f.host = nil
	}
}

func (f *filler) initSubPool() error {
	n := f.opts.Concurrency
	if n < 1 {
		n = defaultConcurrency
	}
	f.subs = make(chan *pooledSub, n)
	for range n {
		s, err := f.newPooledSub()
		if err != nil {
			return err
		}
		f.subs <- s
	}
	return nil
}

func (f *filler) newPooledSub() (*pooledSub, error) {
	slot := &pooledSub{}
	sub, err := f.newSubscriber(slot.put)
	if err != nil {
		return nil, err
	}
	slot.sub = sub
	return slot, nil
}

func (f *filler) closeSubs() {
	if f.subs == nil {
		return
	}
	for {
		select {
		case s := <-f.subs:
			if s != nil && s.sub != nil {
				_ = s.sub.Close()
			}
		default:
			return
		}
	}
}

func (f *filler) takeSub(ctx context.Context) (*pooledSub, error) {
	select {
	case s := <-f.subs:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *filler) putSub(s *pooledSub) {
	s.setOnPut(nil)
	f.subs <- s
}

func (f *filler) waitRebuilds() {
	n := cap(f.subs)
	held := make([]*pooledSub, 0, n)
	for range n {
		held = append(held, <-f.subs)
	}
	for _, s := range held {
		f.subs <- s
	}
}

func (f *filler) fail(err error) {
	f.errMu.Lock()
	defer f.errMu.Unlock()
	if f.err == nil {
		f.err = err
	}
}

func (f *filler) failed() error {
	f.errMu.Lock()
	defer f.errMu.Unlock()
	return f.err
}

func (f *filler) finish(reason string, err error) error {
	f.waitRebuilds()
	if err == nil {
		err = f.failed()
		if err != nil {
			reason = stopError
		}
	}
	if errors.Is(err, context.Canceled) {
		reason = stopCanceled
	}
	f.out.Done(reason, err)
	return err
}

func (f *filler) run(ctx context.Context) error {
	adCid := f.opts.StartAd
	if adCid == cid.Undef {
		err := errors.New("no start advertisement: pass --cid or --indexer so LastAdvertisement can be read from provider info")
		f.out.Done(stopError, err)
		return err
	}

	if f.opts.Estimate {
		if err := f.runEstimate(ctx); err != nil {
			f.out.Done(stopCanceled, err)
			return err
		}
	}

	n := 0
	for adCid != cid.Undef {
		if err := ctx.Err(); err != nil {
			return f.finish(stopCanceled, err)
		}
		if f.opts.Depth > 0 && n >= f.opts.Depth {
			return f.finish(stopDepth, nil)
		}
		if err := f.failed(); err != nil {
			return f.finish(stopError, err)
		}

		n++
		prev, err := f.processAd(ctx, AdRef{N: n, Cid: adCid})
		f.out.Periodic()
		if err != nil {
			return f.finish(stopError, err)
		}
		adCid = prev
	}

	return f.finish(stopGenesis, nil)
}

const estimateSegment = 200

func (f *filler) runEstimate(ctx context.Context) error {
	f.out.Estimating(f.opts.EstimateTimeout)
	estCtx := ctx
	if f.opts.EstimateTimeout > 0 {
		var cancel context.CancelFunc
		estCtx, cancel = context.WithTimeout(ctx, f.opts.EstimateTimeout)
		defer cancel()
	}
	startDown := f.downloaded.Load()
	n, exact, err := f.countAds(estCtx)
	f.downloaded.Store(startDown)
	if err != nil && ctx.Err() != nil {
		f.out.CountComplete(n, false)
		return ctx.Err()
	}
	f.out.CountComplete(n, exact && err == nil)
	return nil
}

func (f *filler) countAds(ctx context.Context) (int, bool, error) {
	sub, err := f.newSubscriber(nil)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = sub.Close() }()

	n := 0
	next := f.opts.StartAd
	for next != cid.Undef {
		if err := ctx.Err(); err != nil {
			return n, false, err
		}
		if f.opts.Depth > 0 && n >= f.opts.Depth {
			return n, true, nil
		}
		limit := estimateSegment
		if f.opts.Depth > 0 && n+limit > f.opts.Depth {
			limit = f.opts.Depth - n
		}
		var lastPrev cid.Cid
		got := 0
		hook := dagsync.MakeGeneralBlockHook(func(adCid cid.Cid) (cid.Cid, error) {
			raw, err := f.ds.Get(ctx, datastore.NewKey(adCid.String()))
			if err != nil {
				return cid.Undef, err
			}
			ad, err := schema.BytesToAdvertisement(adCid, raw)
			if err != nil {
				return cid.Undef, err
			}
			got++
			lastPrev = ad.PreviousCid()
			return lastPrev, nil
		})
		_, err := sub.SyncAdChain(ctx, f.opts.Publisher,
			dagsync.WithHeadAdCid(next),
			dagsync.ScopedDepthLimit(int64(limit)),
			dagsync.ScopedSegmentDepthLimit(int64(limit)),
			dagsync.ScopedBlockHook(hook),
		)
		n += got
		if got > 0 {
			f.out.CountedAds(n)
		}
		if err != nil {
			return n, false, err
		}
		if got == 0 || got < limit {
			return n, true, nil
		}
		next = lastPrev
	}
	return n, true, nil
}

func (f *filler) processAd(ctx context.Context, adRef AdRef) (cid.Cid, error) {
	f.out.CheckingMain(adRef)
	data, src, err := f.loadExisting(ctx, adRef)
	if err != nil {
		return cid.Undef, err
	}

	switch src {
	case sourceMain:
		ad := data.ad
		f.out.Loaded(adRef, src, data)
		if skipUnstored(f.out, adRef, ad, src) {
			return ad.PreviousCid(), nil
		}
		f.out.PresentOnMain(adRef, data)
		return ad.PreviousCid(), nil

	case sourceExternal:
		ad := data.ad
		f.out.Loaded(adRef, src, data)
		if skipUnstored(f.out, adRef, ad, src) {
			return ad.PreviousCid(), nil
		}
		skipEnts := data.hamt || !hasEntries(ad)
		if skipEnts {
			data.entries = nil
		}
		written, err := f.writeFromData(ctx, adRef, data)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot copy %s from external to main: %w", adRef.Cid, err)
		}
		f.out.CopiedFromExternal(adRef, data, written)
		return ad.PreviousCid(), nil
	}

	return f.rebuildFromProvider(ctx, adRef)
}

// rebuildFromProvider fetches the advertisement and its entries from the
// publisher and writes a CAR to main. Used when main and every external
// mirror had no usable CAR.
func (f *filler) rebuildFromProvider(ctx context.Context, adRef AdRef) (cid.Cid, error) {
	slot, err := f.takeSub(ctx)
	if err != nil {
		return cid.Undef, err
	}
	defer func() {
		if slot != nil {
			f.putSub(slot)
		}
	}()

	f.out.NotInMirrorsFetching(adRef, f.opts.Publisher.ID)
	chunkN := 0
	slot.setOnPut(func(c cid.Cid, data []byte) {
		ch, err := decodeEntryChunk(c, data)
		if err != nil {
			return
		}
		chunkN++
		f.out.FetchedEntryChunk(adRef, chunkN, c, len(ch.Entries), len(data), f.downloaded.Load())
		if ch.Next != nil {
			f.out.FetchingEntryChunk(adRef, chunkN+1, ch.Next.(cidlink.Link).Cid)
		}
	})

	ad, err := f.fetchFromProvider(ctx, slot.sub, adRef.Cid)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot fetch %s from provider: %w", adRef.Cid, err)
	}
	f.out.FetchedAd(adRef, ad)
	if skipUnstored(f.out, adRef, ad, sourceProvider) {
		return ad.PreviousCid(), nil
	}

	entsCid := ad.Entries.(cidlink.Link).Cid
	go func(slot *pooledSub, adRef AdRef, entsCid cid.Cid) {
		defer f.putSub(slot)
		if err := f.rebuildEntries(ctx, adRef, slot.sub, entsCid); err != nil {
			f.fail(err)
		}
	}(slot, adRef, entsCid)

	slot = nil
	return ad.PreviousCid(), nil
}

func (f *filler) rebuildEntries(ctx context.Context, adRef AdRef, sub *dagsync.Subscriber, entsCid cid.Cid) error {
	f.out.SyncingFirstEntries(adRef, entsCid)
	if err := sub.SyncOneEntry(ctx, f.opts.Publisher, entsCid); err != nil {
		return fmt.Errorf("cannot sync first entries block for %s: %w", adRef.Cid, err)
	}
	hamt, err := f.entryIsHAMT(ctx, entsCid)
	if err != nil {
		return err
	}
	if hamt {
		f.out.HAMTAdOnly(adRef)
		_ = f.ds.Delete(ctx, datastore.NewKey(entsCid.String()))
		written, err := f.writeMainCAR(ctx, adRef, true, 0)
		if err != nil {
			return fmt.Errorf("cannot write ad-only CAR for HAMT ad %s: %w", adRef.Cid, err)
		}
		f.out.WrittenFromPublisher(adRef, true, 0, 0, written, f.downloaded.Load())
		return nil
	}

	chunks, mhs, err := f.syncRemainingEntries(ctx, sub, entsCid)
	if err != nil {
		return fmt.Errorf("cannot sync entries for %s: %w", adRef.Cid, err)
	}

	written, err := f.writeMainCAR(ctx, adRef, false, chunks)
	if err != nil {
		return fmt.Errorf("cannot write CAR for %s: %w", adRef.Cid, err)
	}
	f.out.WrittenFromPublisher(adRef, false, chunks, mhs, written, f.downloaded.Load())
	return nil
}

func skipUnstored(out Observer, ad AdRef, advertisement schema.Advertisement, src source) bool {
	if advertisement.IsRm {
		out.SkipIsRm(ad, advertisement, src == sourceMain)
		return true
	}
	if !hasEntries(advertisement) {
		out.SkipNoEntries(ad, advertisement)
		return true
	}
	return false
}

func (f *filler) loadExisting(ctx context.Context, ad AdRef) (*carData, source, error) {
	block, err := f.mainReader.Read(ctx, ad.Cid, false)
	switch {
	case err == nil:
		data, vErr := inspectCar(ad.Cid, block)
		if vErr == nil {
			return data, sourceMain, nil
		}
		if isCanceled(vErr) {
			return nil, sourceNone, vErr
		}
		f.out.MainInvalid(ad, vErr)
	case isCanceled(err):
		return nil, sourceNone, err
	case errors.Is(err, fs.ErrNotExist):
		f.out.MainMiss(ad)
	default:
		f.out.MainReadError(ad, err)
	}

	for i, reader := range f.externals {
		f.out.CheckingExternal(ad, i, reader.Location())
		block, err = reader.Read(ctx, ad.Cid, false)
		if err != nil {
			if isCanceled(err) {
				return nil, sourceNone, err
			}
			if errors.Is(err, fs.ErrNotExist) {
				f.out.ExternalMiss(ad, i)
				continue
			}
			f.out.ExternalReadError(ad, i, err)
			continue
		}
		data, vErr := inspectCar(ad.Cid, block)
		if vErr != nil {
			if isCanceled(vErr) {
				return nil, sourceNone, vErr
			}
			f.out.ExternalInvalid(ad, i, vErr)
			continue
		}
		f.out.ExternalHit(ad, i)
		return data, sourceExternal, nil
	}

	return nil, sourceNone, nil
}

func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func inspectCar(adCid cid.Cid, block *carstore.AdBlock) (*carData, error) {
	defer func() { _ = block.Close() }()

	if err := verifyCID(adCid, block.Data); err != nil {
		return nil, fmt.Errorf("advertisement blob: %w", err)
	}
	ad, err := block.Advertisement()
	if err != nil {
		return nil, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	out := &carData{
		ad:     ad,
		adData: block.Data,
		size:   int64(len(block.Data)),
	}

	if !hasEntries(ad) {
		if block.Entries == nil {
			return out, nil
		}
		var extra int
		for range block.Entries {
			extra++
		}
		if extra > 0 {
			return nil, fmt.Errorf("CAR has %d extra blocks but advertisement has no entries", extra)
		}
		return out, nil
	}

	expected := ad.Entries.(cidlink.Link).Cid
	for entry := range block.Entries {
		if entry.Err != nil {
			return nil, entry.Err
		}
		if expected == cid.Undef {
			return nil, errors.New("CAR has extra entry blocks beyond the entries chain")
		}
		if entry.Cid != expected {
			return nil, fmt.Errorf("entry CID mismatch: car has %s, chain wants %s", entry.Cid, expected)
		}
		if err := verifyCID(entry.Cid, entry.Data); err != nil {
			return nil, fmt.Errorf("entry blob %s: %w", entry.Cid, err)
		}

		chunk, err := entry.EntryChunk()
		if err != nil {
			if errors.Is(err, carstore.ErrHAMT) {
				out.hamt = true
				for range block.Entries {
				}
				return out, nil
			}
			return nil, fmt.Errorf("cannot decode entry chunk %s: %w", entry.Cid, err)
		}

		out.entries = append(out.entries, carstore.EntryBlock{Cid: entry.Cid, Data: entry.Data})
		out.chunks++
		out.mhs += len(chunk.Entries)
		out.size += int64(len(entry.Data))
		if chunk.Next == nil {
			expected = cid.Undef
		} else {
			expected = chunk.Next.(cidlink.Link).Cid
		}
	}
	if expected != cid.Undef {
		return nil, fmt.Errorf("CAR is missing remaining entries starting at %s", expected)
	}
	return out, nil
}

func (f *filler) writeFromData(ctx context.Context, ad AdRef, data *carData) (int64, error) {
	if err := f.ds.Put(ctx, datastore.NewKey(ad.Cid.String()), data.adData); err != nil {
		return 0, err
	}
	skipEnts := data.hamt || !hasEntries(data.ad)
	chunks := 0
	if !skipEnts {
		chunks = data.chunks
		for _, e := range data.entries {
			if err := f.ds.Put(ctx, datastore.NewKey(e.Cid.String()), e.Data); err != nil {
				return 0, err
			}
		}
	}
	return f.writeMainCAR(ctx, ad, skipEnts, chunks)
}

func (f *filler) writeMainCAR(ctx context.Context, ad AdRef, skipEnts bool, chunks int) (int64, error) {
	f.out.WritingCAR(ad, chunks)
	info, err := f.mainWriter.Write(ctx, ad.Cid, skipEnts, false)
	if err != nil {
		return 0, err
	}
	if info == nil {
		return 0, nil
	}
	return info.Size, nil
}

func (f *filler) fetchFromProvider(ctx context.Context, sub *dagsync.Subscriber, adCid cid.Cid) (schema.Advertisement, error) {
	_, err := sub.SyncAdChain(ctx, f.opts.Publisher, dagsync.WithHeadAdCid(adCid), dagsync.ScopedDepthLimit(1))
	if err != nil {
		return schema.Advertisement{}, err
	}
	raw, err := f.ds.Get(ctx, datastore.NewKey(adCid.String()))
	if err != nil {
		return schema.Advertisement{}, err
	}
	return schema.BytesToAdvertisement(adCid, raw)
}

func (f *filler) syncRemainingEntries(ctx context.Context, sub *dagsync.Subscriber, first cid.Cid) (chunks, mhs int, err error) {
	raw, err := f.ds.Get(ctx, datastore.NewKey(first.String()))
	if err != nil {
		return 0, 0, err
	}
	chunk, err := decodeEntryChunk(first, raw)
	if err != nil {
		return 0, 0, err
	}
	chunks = 1
	mhs = len(chunk.Entries)
	if chunk.Next == nil {
		return chunks, mhs, nil
	}

	next := chunk.Next.(cidlink.Link).Cid
	hook := func(_ peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
		raw, err := f.ds.Get(ctx, datastore.NewKey(c.String()))
		if err != nil {
			actions.FailSync(err)
			return
		}
		ch, err := decodeEntryChunk(c, raw)
		if err != nil {
			actions.FailSync(err)
			return
		}
		chunks++
		mhs += len(ch.Entries)
		if ch.Next == nil {
			actions.SetNextSyncCid(cid.Undef)
			return
		}
		actions.SetNextSyncCid(ch.Next.(cidlink.Link).Cid)
	}
	opts := []dagsync.SyncOption{dagsync.ScopedBlockHook(hook)}
	if f.opts.EntriesDepthLimit != 0 {
		opts = append(opts, dagsync.ScopedDepthLimit(f.opts.EntriesDepthLimit))
	}
	if err = sub.SyncEntries(ctx, f.opts.Publisher, next, opts...); err != nil {
		return chunks, mhs, err
	}
	return chunks, mhs, nil
}

func (f *filler) entryIsHAMT(ctx context.Context, entsCid cid.Cid) (bool, error) {
	raw, err := f.ds.Get(ctx, datastore.NewKey(entsCid.String()))
	if err != nil {
		return false, err
	}
	node, err := decodeNode(entsCid, raw)
	if err != nil {
		return false, err
	}
	return isHAMT(node), nil
}

func (f *filler) initHost() error {
	h, err := libp2p.New()
	if err != nil {
		return fmt.Errorf("cannot create libp2p host: %w", err)
	}
	if f.opts.Publisher.ID != "" && len(f.opts.Publisher.Addrs) > 0 {
		h.Peerstore().AddAddrs(f.opts.Publisher.ID, f.opts.Publisher.Addrs, time.Hour)
	}
	f.host = h
	return nil
}

func (f *filler) newSubscriber(onPut func(cid.Cid, []byte)) (*dagsync.Subscriber, error) {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := f.ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			b := buf.Bytes()
			f.downloaded.Add(int64(len(b)))
			if err := f.ds.Put(lctx.Ctx, datastore.NewKey(c.String()), b); err != nil {
				return err
			}
			if onPut != nil {
				onPut(c, b)
			}
			return nil
		}, nil
	}

	subOpts := []dagsync.Option{
		dagsync.HttpTimeout(f.opts.HttpTimeout),
		dagsync.RetryableHTTPClient(f.opts.HttpRetryMax, f.opts.HttpRetryWaitMin, f.opts.HttpRetryWaitMax),
	}
	if f.opts.EntriesDepthLimit != 0 {
		subOpts = append(subOpts, dagsync.EntriesDepthLimit(f.opts.EntriesDepthLimit))
	}
	sub, err := dagsync.NewSubscriber(f.host, lsys, subOpts...)
	if err != nil {
		return nil, fmt.Errorf("cannot create dagsync subscriber: %w", err)
	}
	return sub, nil
}

func hasEntries(ad schema.Advertisement) bool {
	if ad.Entries == nil || ad.Entries == schema.NoEntries {
		return false
	}
	c, ok := ad.Entries.(cidlink.Link)
	if !ok {
		return false
	}
	return c.Cid != cid.Undef
}

func verifyCID(c cid.Cid, data []byte) error {
	got, err := c.Prefix().Sum(data)
	if err != nil {
		return err
	}
	if !got.Equals(c) {
		return fmt.Errorf("cid does not match data (got %s want %s)", got, c)
	}
	return nil
}

func decodeEntryChunk(c cid.Cid, data []byte) (*schema.EntryChunk, error) {
	node, err := decodeNodeWithPrototype(c, data, schema.EntryChunkPrototype)
	if err != nil {
		return nil, err
	}
	chunk, err := schema.UnwrapEntryChunk(node)
	if err != nil {
		return nil, err
	}
	return chunk, nil
}

func decodeNode(c cid.Cid, data []byte) (ipld.Node, error) {
	return decodeNodeWithPrototype(c, data, basicnode.Prototype.Any)
}

func decodeNodeWithPrototype(c cid.Cid, data []byte, proto ipld.NodePrototype) (ipld.Node, error) {
	nb := proto.NewBuilder()
	decoder, err := multicodec.LookupDecoder(c.Prefix().Codec)
	if err != nil {
		return nil, err
	}
	if err = decoder(nb, bytes.NewBuffer(data)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

func isHAMT(n ipld.Node) bool {
	h, _ := n.LookupByString("hamt")
	return h != nil
}
