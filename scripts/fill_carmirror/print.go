package main

import (
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/peer"
)

const printTimeFormat = "2006-01-02T15:04:05.000-0700"

// PrintProgress writes timestamped lines in the original fill_carmirror style.
type PrintProgress struct {
	counts
	w   io.Writer
	now func() time.Time
}

func NewPrintProgress(w io.Writer) *PrintProgress {
	return &PrintProgress{w: w, now: time.Now}
}

func (p *PrintProgress) ts() string {
	now := p.now
	if now == nil {
		now = time.Now
	}
	return now().Format(printTimeFormat)
}

func (p *PrintProgress) line(format string, args ...any) {
	fmt.Fprintf(p.w, "%s  "+format+"\n", append([]any{p.ts()}, args...)...)
}

func (p *PrintProgress) adLine(ad AdRef, format string, args ...any) {
	p.line("%s %s  "+format, append([]any{formatAdIndex(ad.N, p.total, p.exact), ad.Cid}, args...)...)
}

func (p *PrintProgress) Start(opts Options) {
	defer p.locked()()
	p.line("Filling car mirror for provider %s", opts.Provider)
	if opts.StartAd != cid.Undef {
		p.line("Start ad: %s", opts.StartAd)
	}
	if opts.Publisher.ID != "" {
		p.line("Publisher: %s %s", opts.Publisher.ID, opts.Publisher.Addrs)
	}
	p.line("MainMode: %s", opts.Mirror.MainMode)
	p.line("Main mirror: %s", opts.Mirror.Main.Local.BasePath)
	for i, ext := range opts.Mirror.External {
		loc := ext.HTTP.BaseURL
		if loc == "" {
			loc = ext.Local.BasePath
		}
		p.line("External[%d]: %s %s", i, ext.Type, loc)
	}
}

func (p *PrintProgress) UsingIndexer(startAd cid.Cid, indexerURL string) {
	defer p.locked()()
	p.line("using LastAdvertisement from indexer %s: %s", indexerURL, startAd)
}

func (p *PrintProgress) Estimating(timeout time.Duration) {
	defer p.locked()()
	if timeout > 0 {
		p.line("counting advertisements in chain (timeout %s)", timeout)
		return
	}
	p.line("counting advertisements in chain")
}

func (p *PrintProgress) CountedAds(n int) {
	defer p.locked()()
	p.line("counted %d advertisements so far", n)
}

func (p *PrintProgress) CountComplete(total int, exact bool) {
	defer p.locked()()
	p.noteCountComplete(total, exact)
	if exact {
		p.line("chain has %d advertisements", total)
		return
	}
	p.line("chain has at least %d advertisements (count incomplete)", total)
}

func (p *PrintProgress) Periodic() {
	defer p.locked()()
	p.line("progress  scanned=%s present=%d external=%d downloaded=%d rm=%d hamt=%d chunks=%d mhs=%d down_bytes=%d written=%d last=%s",
		formatScanned(p.scanned, p.total, p.exact), p.present, p.copied, p.downloaded, p.skippedRm, p.skippedHAMT,
		p.chunks, p.mhs, p.downBytes, p.written, p.lastAd)
}

func (p *PrintProgress) Done(reason string, err error) {
	defer p.locked()()
	p.noteDone(reason)
	p.line("Stats:")
	fmt.Fprintf(p.w, "  scanned:           %d\n", p.scanned)
	fmt.Fprintf(p.w, "  already present:   %d\n", p.present)
	fmt.Fprintf(p.w, "  copied (external): %d\n", p.copied)
	fmt.Fprintf(p.w, "  downloaded:        %d\n", p.downloaded)
	fmt.Fprintf(p.w, "  skipped HAMT:      %d\n", p.skippedHAMT)
	fmt.Fprintf(p.w, "  skipped no ents:   %d\n", p.skippedNoEnts)
	fmt.Fprintf(p.w, "  skipped IsRm:      %d\n", p.skippedRm)
	fmt.Fprintf(p.w, "  entry chunks:      %d\n", p.chunks)
	fmt.Fprintf(p.w, "  multihashes:       %d\n", p.mhs)
	fmt.Fprintf(p.w, "  bytes downloaded:  %d\n", p.downBytes)
	fmt.Fprintf(p.w, "  bytes written:     %d\n", p.written)
	if p.lastAd != cid.Undef {
		fmt.Fprintf(p.w, "  last ad:           %s\n", p.lastAd)
	}
	if p.stop != "" {
		fmt.Fprintf(p.w, "  stop reason:       %s\n", p.stop)
	}
}

func (p *PrintProgress) CheckingMain(ad AdRef) {
	defer p.locked()()
	p.noteChecking(ad)
	p.adLine(ad, "checking main")
}
func (p *PrintProgress) MainInvalid(ad AdRef, err error) {
	defer p.locked()()
	p.adLine(ad, "main CAR invalid, will recreate: %s", err)
}
func (p *PrintProgress) MainMiss(ad AdRef) {
	defer p.locked()()
	p.adLine(ad, "main miss")
}
func (p *PrintProgress) MainReadError(ad AdRef, err error) {
	defer p.locked()()
	p.adLine(ad, "main read error, will recreate: %s", err)
}
func (p *PrintProgress) CheckingExternal(ad AdRef, i int, loc string) {
	defer p.locked()()
	p.adLine(ad, "checking external[%d] %s", i, loc)
}
func (p *PrintProgress) ExternalMiss(ad AdRef, i int) {
	defer p.locked()()
	p.adLine(ad, "external[%d] miss", i)
}
func (p *PrintProgress) ExternalReadError(ad AdRef, i int, err error) {
	defer p.locked()()
	p.adLine(ad, "external[%d] read error: %s", i, err)
}
func (p *PrintProgress) ExternalInvalid(ad AdRef, i int, err error) {
	defer p.locked()()
	p.adLine(ad, "external[%d] invalid CAR: %s", i, err)
}
func (p *PrintProgress) ExternalHit(ad AdRef, i int) {
	defer p.locked()()
	p.adLine(ad, "external[%d] hit", i)
}
func (p *PrintProgress) Loaded(ad AdRef, src source, data *carData) {
	defer p.locked()()
	p.adLine(ad, "loaded from %s  %s", src, formatCarData(data))
}
func (p *PrintProgress) NotInMirrorsFetching(ad AdRef, publisher peer.ID) {
	defer p.locked()()
	p.adLine(ad, "not in mirrors, fetching from publisher %s", publisher)
}
func (p *PrintProgress) FetchedAd(ad AdRef, advertisement schema.Advertisement) {
	defer p.locked()()
	p.adLine(ad, "fetched ad  %s", formatAd(advertisement))
}
func (p *PrintProgress) SkipIsRm(ad AdRef, advertisement schema.Advertisement, carOnMain bool) {
	defer p.locked()()
	p.noteSkipRm()
	p.adLine(ad, "skip IsRm  car_on_main=%t  %s", carOnMain, formatAd(advertisement))
}
func (p *PrintProgress) SkipNoEntries(ad AdRef, advertisement schema.Advertisement) {
	defer p.locked()()
	p.noteSkipNoEnts()
	p.adLine(ad, "skip no-entries  %s", formatAd(advertisement))
}
func (p *PrintProgress) PresentOnMain(ad AdRef, data *carData) {
	defer p.locked()()
	p.notePresent(data)
	p.adLine(ad, "present on main  %s", formatCarData(data))
}
func (p *PrintProgress) CopiedFromExternal(ad AdRef, data *carData, written int64) {
	defer p.locked()()
	p.noteCopied(data, written)
	p.adLine(ad, "copied from external  %s  written=%d", formatCarData(data), written)
}
func (p *PrintProgress) SyncingFirstEntries(ad AdRef, entsCid cid.Cid) {
	defer p.locked()()
	p.adLine(ad, "syncing first entries block %s", entsCid)
}
func (p *PrintProgress) HAMTAdOnly(ad AdRef) {
	defer p.locked()()
	p.adLine(ad, "entries are HAMT, writing ad only")
}
func (p *PrintProgress) FetchingEntryChunk(ad AdRef, n int, chunkCid cid.Cid) {
	defer p.locked()()
	p.adLine(ad, "fetching entry chunk %d  %s", n, chunkCid)
}
func (p *PrintProgress) FetchedEntryChunk(ad AdRef, n int, chunkCid cid.Cid, mhs, chunkBytes int, downBytes int64) {
	defer p.locked()()
	p.adLine(ad, "got entry chunk %d  %s  mhs=%d bytes=%d down_bytes=%d", n, chunkCid, mhs, chunkBytes, downBytes)
}
func (p *PrintProgress) WritingCAR(ad AdRef, chunks int) {
	defer p.locked()()
	if chunks <= 0 {
		p.adLine(ad, "writing CAR (ad only)")
		return
	}
	p.adLine(ad, "writing CAR (%d chunks)", chunks)
}
func (p *PrintProgress) WrittenFromPublisher(ad AdRef, hamt bool, chunks, mhs int, written, downBytes int64) {
	defer p.locked()()
	p.noteDownloaded(hamt, chunks, mhs, written, downBytes)
	if hamt {
		p.adLine(ad, "downloaded (HAMT skipped)  written=%d down_bytes=%d", written, downBytes)
		return
	}
	p.adLine(ad, "downloaded  chunks=%d mhs=%d written=%d down_bytes=%d", chunks, mhs, written, downBytes)
}
