package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	stopCanceled = "canceled"
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

	// Progress is called with a copy of the running stats. Optional.
	Progress func(Stats)
}

// Stats is the running / final summary of a fill run.
type Stats struct {
	Scanned         int
	AlreadyPresent  int
	CopiedExternal  int
	Downloaded      int
	Recreated       int
	SkippedHAMT     int
	SkippedNoEnts   int
	SkippedRm       int
	EntryChunks     int
	Multihashes     int
	BytesDownloaded int64
	BytesWritten    int64
	StopReason      string
	LastAd          cid.Cid
}

func (s Stats) print() {
	fmt.Println("Stats:")
	fmt.Printf("  scanned:           %d\n", s.Scanned)
	fmt.Printf("  already present:   %d\n", s.AlreadyPresent)
	fmt.Printf("  copied (external): %d\n", s.CopiedExternal)
	fmt.Printf("  downloaded:        %d\n", s.Downloaded)
	fmt.Printf("  recreated:         %d\n", s.Recreated)
	fmt.Printf("  skipped HAMT:      %d\n", s.SkippedHAMT)
	fmt.Printf("  skipped no ents:   %d\n", s.SkippedNoEnts)
	fmt.Printf("  skipped IsRm:      %d\n", s.SkippedRm)
	fmt.Printf("  entry chunks:      %d\n", s.EntryChunks)
	fmt.Printf("  multihashes:       %d\n", s.Multihashes)
	fmt.Printf("  bytes downloaded:  %d\n", s.BytesDownloaded)
	fmt.Printf("  bytes written:     %d\n", s.BytesWritten)
	if s.LastAd != cid.Undef {
		fmt.Printf("  last ad:           %s\n", s.LastAd)
	}
	if s.StopReason != "" {
		fmt.Printf("  stop reason:       %s\n", s.StopReason)
	}
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
	ds         datastore.Batching
	mainReader *carstore.CarReader
	mainWriter *carstore.CarWriter
	externals  []*carstore.CarReader
	downloaded atomic.Int64

	host host.Host
	sub  *dagsync.Subscriber
}

// Fill walks a provider's advertisement chain and writes missing or invalid
// CAR files to the main advertisement mirror. Removal (IsRm) and no-entries
// ads are not stored. An IsRm ad is reported (whether a CAR already exists on
// main) and otherwise left untouched. It never opens the indexer value store.
func Fill(ctx context.Context, opts Options) (*Stats, error) {
	f, err := newFiller(opts)
	if err != nil {
		return nil, err
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

	return &filler{
		opts:       opts,
		ds:         ds,
		mainReader: mainReader,
		mainWriter: mainWriter,
		externals:  externals,
	}, nil
}

func (f *filler) close() {
	if f.sub != nil {
		_ = f.sub.Close()
	}
	if f.host != nil {
		_ = f.host.Close()
	}
}

func (f *filler) run(ctx context.Context) (*Stats, error) {
	st := &Stats{}
	adCid := f.opts.StartAd
	if adCid == cid.Undef {
		return st, errors.New("no start advertisement: pass --cid or --indexer so LastAdvertisement can be read from provider info")
	}

	for adCid != cid.Undef {
		if err := ctx.Err(); err != nil {
			st.StopReason = stopCanceled
			return st, err
		}
		if f.opts.Depth > 0 && st.Scanned >= f.opts.Depth {
			st.StopReason = stopDepth
			return st, nil
		}

		prev, err := f.processAd(ctx, adCid, st)
		st.Scanned++
		st.LastAd = adCid
		if f.opts.Progress != nil {
			f.opts.Progress(*st)
		}
		if err != nil {
			if st.StopReason == "" {
				st.StopReason = stopError
			}
			return st, err
		}
		adCid = prev
	}

	st.StopReason = stopGenesis
	return st, nil
}

func (f *filler) processAd(ctx context.Context, adCid cid.Cid, st *Stats) (cid.Cid, error) {
	n := st.Scanned + 1
	fmt.Printf("[%d] %s  checking main\n", n, adCid)
	data, src, mainBroken, err := f.loadExisting(ctx, n, adCid)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return cid.Undef, err
	}

	var ad schema.Advertisement
	if err == nil {
		ad = data.ad
		fmt.Printf("[%d] %s  loaded from %s  %s\n", n, adCid, src, formatCarData(data))
	} else {
		if mainBroken {
			fmt.Printf("[%d] %s  main CAR unusable, fetching from publisher %s\n", n, adCid, f.opts.Publisher.ID)
		} else {
			fmt.Printf("[%d] %s  not in mirrors, fetching from publisher %s\n", n, adCid, f.opts.Publisher.ID)
		}
		ad, err = f.fetchFromProvider(ctx, adCid)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot fetch %s from provider: %w", adCid, err)
		}
		src = sourceProvider
		st.BytesDownloaded = f.downloaded.Load()
		fmt.Printf("[%d] %s  fetched ad  %s\n", n, adCid, formatAd(ad))
	}

	if skipUnstored(n, adCid, ad, src, mainBroken, st) {
		return ad.PreviousCid(), nil
	}

	switch src {
	case sourceMain:
		st.AlreadyPresent++
		st.EntryChunks += data.chunks
		st.Multihashes += data.mhs
		if data.hamt {
			st.SkippedHAMT++
		}
		fmt.Printf("[%d] %s  present on main  %s\n", n, adCid, formatCarData(data))
		return ad.PreviousCid(), nil

	case sourceExternal:
		skipEnts := data.hamt || !hasEntries(ad)
		if skipEnts {
			data.entries = nil
		}
		written, err := f.writeFromData(ctx, adCid, data)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot copy %s from external to main: %w", adCid, err)
		}
		st.CopiedExternal++
		st.BytesWritten += written
		if mainBroken {
			st.Recreated++
		}
		if data.hamt {
			st.SkippedHAMT++
		} else {
			st.EntryChunks += data.chunks
			st.Multihashes += data.mhs
		}
		action := "copied from external"
		if mainBroken {
			action = "recreated from external"
		}
		fmt.Printf("[%d] %s  %s  %s  written=%d\n", n, adCid, action, formatCarData(data), written)
		return ad.PreviousCid(), nil
	}

	entsCid := ad.Entries.(cidlink.Link).Cid
	fmt.Printf("[%d] %s  syncing first entries block %s\n", n, adCid, entsCid)
	if err = f.syncOneEntry(ctx, entsCid); err != nil {
		return cid.Undef, fmt.Errorf("cannot sync first entries block for %s: %w", adCid, err)
	}
	hamt, err := f.entryIsHAMT(ctx, entsCid)
	if err != nil {
		return cid.Undef, err
	}
	if hamt {
		fmt.Printf("[%d] %s  entries are HAMT, writing ad only\n", n, adCid)
		_ = f.ds.Delete(ctx, datastore.NewKey(entsCid.String()))
		written, err := f.writeAdOnly(ctx, adCid)
		if err != nil {
			return cid.Undef, fmt.Errorf("cannot write ad-only CAR for HAMT ad %s: %w", adCid, err)
		}
		st.SkippedHAMT++
		noteWrittenFromPublisher(st, mainBroken)
		st.BytesWritten += written
		st.BytesDownloaded = f.downloaded.Load()
		fmt.Printf("[%d] %s  %s (HAMT skipped)  written=%d down_bytes=%d\n", n, adCid, publisherAction(mainBroken), written, st.BytesDownloaded)
		return ad.PreviousCid(), nil
	}

	fmt.Printf("[%d] %s  syncing remaining entry chunks from %s\n", n, adCid, entsCid)
	chunks, mhs, err := f.syncRemainingEntries(ctx, entsCid)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot sync entries for %s: %w", adCid, err)
	}

	info, err := f.mainWriter.Write(ctx, adCid, false, false)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot write CAR for %s: %w", adCid, err)
	}
	noteWrittenFromPublisher(st, mainBroken)
	st.EntryChunks += chunks
	st.Multihashes += mhs
	st.BytesDownloaded = f.downloaded.Load()
	var written int64
	if info != nil {
		written = info.Size
		st.BytesWritten += written
	}
	fmt.Printf("[%d] %s  %s  %s  chunks=%d mhs=%d written=%d down_bytes=%d\n", n, adCid, publisherAction(mainBroken), formatAd(ad), chunks, mhs, written, st.BytesDownloaded)
	return ad.PreviousCid(), nil
}

func skipUnstored(n int, adCid cid.Cid, ad schema.Advertisement, src source, mainBroken bool, st *Stats) bool {
	if ad.IsRm {
		onMain := src == sourceMain || mainBroken
		st.SkippedRm++
		fmt.Printf("[%d] %s  skip IsRm  car_on_main=%t  %s\n", n, adCid, onMain, formatAd(ad))
		return true
	}
	if !hasEntries(ad) {
		st.SkippedNoEnts++
		fmt.Printf("[%d] %s  skip no-entries  %s\n", n, adCid, formatAd(ad))
		return true
	}
	return false
}

func noteWrittenFromPublisher(st *Stats, mainBroken bool) {
	st.Downloaded++
	if mainBroken {
		st.Recreated++
	}
}

func publisherAction(mainBroken bool) string {
	if mainBroken {
		return "recreated from publisher"
	}
	return "downloaded"
}

func (f *filler) loadExisting(ctx context.Context, n int, adCid cid.Cid) (*carData, source, bool, error) {
	mainBroken := false
	block, err := f.mainReader.Read(ctx, adCid, false)
	switch {
	case err == nil:
		data, vErr := inspectCar(adCid, block)
		if vErr == nil {
			return data, sourceMain, false, nil
		}
		if isCanceled(vErr) {
			return nil, sourceNone, false, vErr
		}
		fmt.Printf("[%d] %s  main CAR invalid, will recreate: %s\n", n, adCid, vErr)
		mainBroken = true
	case isCanceled(err):
		return nil, sourceNone, false, err
	case errors.Is(err, fs.ErrNotExist):
		fmt.Printf("[%d] %s  main miss\n", n, adCid)
	default:
		fmt.Printf("[%d] %s  main read error, will recreate: %s\n", n, adCid, err)
		mainBroken = true
	}

	for i, reader := range f.externals {
		fmt.Printf("[%d] %s  checking external[%d] %s\n", n, adCid, i, reader.Location())
		block, err = reader.Read(ctx, adCid, false)
		if err != nil {
			if isCanceled(err) {
				return nil, sourceNone, mainBroken, err
			}
			if errors.Is(err, fs.ErrNotExist) {
				fmt.Printf("[%d] %s  external[%d] miss\n", n, adCid, i)
				continue
			}
			fmt.Printf("[%d] %s  external[%d] read error: %s\n", n, adCid, i, err)
			continue
		}
		data, vErr := inspectCar(adCid, block)
		if vErr != nil {
			if isCanceled(vErr) {
				return nil, sourceNone, mainBroken, vErr
			}
			fmt.Printf("[%d] %s  external[%d] invalid CAR: %s\n", n, adCid, i, vErr)
			continue
		}
		fmt.Printf("[%d] %s  external[%d] hit\n", n, adCid, i)
		return data, sourceExternal, mainBroken, nil
	}

	return nil, sourceNone, mainBroken, fs.ErrNotExist
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

func (f *filler) writeFromData(ctx context.Context, adCid cid.Cid, data *carData) (int64, error) {
	if err := f.ds.Put(ctx, datastore.NewKey(adCid.String()), data.adData); err != nil {
		return 0, err
	}
	skipEnts := data.hamt || !hasEntries(data.ad)
	if !skipEnts {
		for _, e := range data.entries {
			if err := f.ds.Put(ctx, datastore.NewKey(e.Cid.String()), e.Data); err != nil {
				return 0, err
			}
		}
	}
	info, err := f.mainWriter.Write(ctx, adCid, skipEnts, false)
	if err != nil {
		return 0, err
	}
	if info == nil {
		return 0, nil
	}
	return info.Size, nil
}

func (f *filler) writeAdOnly(ctx context.Context, adCid cid.Cid) (int64, error) {
	info, err := f.mainWriter.Write(ctx, adCid, true, false)
	if err != nil {
		return 0, err
	}
	if info == nil {
		return 0, nil
	}
	return info.Size, nil
}

func (f *filler) fetchFromProvider(ctx context.Context, adCid cid.Cid) (schema.Advertisement, error) {
	if err := f.ensurePublisher(ctx); err != nil {
		return schema.Advertisement{}, err
	}
	_, err := f.sub.SyncAdChain(ctx, f.opts.Publisher, dagsync.WithHeadAdCid(adCid), dagsync.ScopedDepthLimit(1))
	if err != nil {
		return schema.Advertisement{}, err
	}
	raw, err := f.ds.Get(ctx, datastore.NewKey(adCid.String()))
	if err != nil {
		return schema.Advertisement{}, err
	}
	return schema.BytesToAdvertisement(adCid, raw)
}

func (f *filler) syncOneEntry(ctx context.Context, entsCid cid.Cid) error {
	if err := f.ensurePublisher(ctx); err != nil {
		return err
	}
	return f.sub.SyncOneEntry(ctx, f.opts.Publisher, entsCid)
}

func (f *filler) syncRemainingEntries(ctx context.Context, first cid.Cid) (chunks, mhs int, err error) {
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
	if err = f.sub.SyncEntries(ctx, f.opts.Publisher, next, opts...); err != nil {
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

func (f *filler) ensurePublisher(ctx context.Context) error {
	if f.sub != nil {
		return nil
	}
	if f.opts.Publisher.ID == "" && len(f.opts.Publisher.Addrs) == 0 {
		return errors.New("no publisher address: pass --addr-info or --indexer")
	}

	h, err := libp2p.New()
	if err != nil {
		return fmt.Errorf("cannot create libp2p host: %w", err)
	}
	if f.opts.Publisher.ID != "" && len(f.opts.Publisher.Addrs) > 0 {
		h.Peerstore().AddAddrs(f.opts.Publisher.ID, f.opts.Publisher.Addrs, time.Hour)
	}

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
			return f.ds.Put(lctx.Ctx, datastore.NewKey(c.String()), b)
		}, nil
	}

	subOpts := []dagsync.Option{
		dagsync.HttpTimeout(f.opts.HttpTimeout),
		dagsync.RetryableHTTPClient(f.opts.HttpRetryMax, f.opts.HttpRetryWaitMin, f.opts.HttpRetryWaitMax),
	}
	if f.opts.EntriesDepthLimit != 0 {
		subOpts = append(subOpts, dagsync.EntriesDepthLimit(f.opts.EntriesDepthLimit))
	}
	sub, err := dagsync.NewSubscriber(h, lsys, subOpts...)
	if err != nil {
		_ = h.Close()
		return fmt.Errorf("cannot create dagsync subscriber: %w", err)
	}

	f.host = h
	f.sub = sub
	return nil
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

func formatAd(ad schema.Advertisement) string {
	prev := "nil"
	if p := ad.PreviousCid(); p != cid.Undef {
		prev = p.String()
	}
	ents := "none"
	if hasEntries(ad) {
		ents = ad.Entries.(cidlink.Link).Cid.String()
	}
	rm := ""
	if ad.IsRm {
		rm = " rm=true"
	}
	return fmt.Sprintf("prev=%s entries=%s provider=%s%s", prev, ents, ad.Provider, rm)
}

func formatCarData(data *carData) string {
	kind := "entries"
	if data.hamt {
		kind = "HAMT"
	} else if !hasEntries(data.ad) {
		kind = "no-entries"
	}
	return fmt.Sprintf("%s  %s  chunks=%d mhs=%d car_bytes=%d", formatAd(data.ad), kind, data.chunks, data.mhs, data.size)
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
