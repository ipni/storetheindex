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

func (p *PrintProgress) Start(opts Options) {
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
	p.line("using LastAdvertisement from indexer %s: %s", indexerURL, startAd)
}

func (p *PrintProgress) Ad(n int, adCid cid.Cid) AdProgress {
	return printAd{p: p, n: n, ad: adCid}
}

func (p *PrintProgress) Periodic(s Stats) {
	p.line("progress  scanned=%d present=%d external=%d downloaded=%d recreated=%d rm=%d hamt=%d chunks=%d mhs=%d down_bytes=%d written=%d last=%s",
		s.Scanned, s.AlreadyPresent, s.CopiedExternal, s.Downloaded, s.Recreated, s.SkippedRm, s.SkippedHAMT,
		s.EntryChunks, s.Multihashes, s.BytesDownloaded, s.BytesWritten, s.LastAd)
}

func (p *PrintProgress) Done(s Stats, err error) {
	p.line("Stats:")
	fmt.Fprintf(p.w, "  scanned:           %d\n", s.Scanned)
	fmt.Fprintf(p.w, "  already present:   %d\n", s.AlreadyPresent)
	fmt.Fprintf(p.w, "  copied (external): %d\n", s.CopiedExternal)
	fmt.Fprintf(p.w, "  downloaded:        %d\n", s.Downloaded)
	fmt.Fprintf(p.w, "  recreated:         %d\n", s.Recreated)
	fmt.Fprintf(p.w, "  skipped HAMT:      %d\n", s.SkippedHAMT)
	fmt.Fprintf(p.w, "  skipped no ents:   %d\n", s.SkippedNoEnts)
	fmt.Fprintf(p.w, "  skipped IsRm:      %d\n", s.SkippedRm)
	fmt.Fprintf(p.w, "  entry chunks:      %d\n", s.EntryChunks)
	fmt.Fprintf(p.w, "  multihashes:       %d\n", s.Multihashes)
	fmt.Fprintf(p.w, "  bytes downloaded:  %d\n", s.BytesDownloaded)
	fmt.Fprintf(p.w, "  bytes written:     %d\n", s.BytesWritten)
	if s.LastAd != cid.Undef {
		fmt.Fprintf(p.w, "  last ad:           %s\n", s.LastAd)
	}
	if s.StopReason != "" {
		fmt.Fprintf(p.w, "  stop reason:       %s\n", s.StopReason)
	}
}

type printAd struct {
	p  *PrintProgress
	n  int
	ad cid.Cid
}

func (a printAd) line(format string, args ...any) {
	a.p.line("[%d] %s  "+format, append([]any{a.n, a.ad}, args...)...)
}

func (a printAd) CheckingMain() { a.line("checking main") }
func (a printAd) MainInvalid(err error) {
	a.line("main CAR invalid, will recreate: %s", err)
}
func (a printAd) MainMiss() { a.line("main miss") }
func (a printAd) MainReadError(err error) {
	a.line("main read error, will recreate: %s", err)
}
func (a printAd) CheckingExternal(i int, loc string) {
	a.line("checking external[%d] %s", i, loc)
}
func (a printAd) ExternalMiss(i int) { a.line("external[%d] miss", i) }
func (a printAd) ExternalReadError(i int, err error) {
	a.line("external[%d] read error: %s", i, err)
}
func (a printAd) ExternalInvalid(i int, err error) {
	a.line("external[%d] invalid CAR: %s", i, err)
}
func (a printAd) ExternalHit(i int) { a.line("external[%d] hit", i) }
func (a printAd) Loaded(src source, data *carData) {
	a.line("loaded from %s  %s", src, formatCarData(data))
}
func (a printAd) MainUnusableFetching(publisher peer.ID) {
	a.line("main CAR unusable, fetching from publisher %s", publisher)
}
func (a printAd) NotInMirrorsFetching(publisher peer.ID) {
	a.line("not in mirrors, fetching from publisher %s", publisher)
}
func (a printAd) FetchedAd(ad schema.Advertisement) {
	a.line("fetched ad  %s", formatAd(ad))
}
func (a printAd) SkipIsRm(ad schema.Advertisement, carOnMain bool) {
	a.line("skip IsRm  car_on_main=%t  %s", carOnMain, formatAd(ad))
}
func (a printAd) SkipNoEntries(ad schema.Advertisement) {
	a.line("skip no-entries  %s", formatAd(ad))
}
func (a printAd) PresentOnMain(data *carData) {
	a.line("present on main  %s", formatCarData(data))
}
func (a printAd) CopiedFromExternal(data *carData, written int64, recreated bool) {
	action := "copied from external"
	if recreated {
		action = "recreated from external"
	}
	a.line("%s  %s  written=%d", action, formatCarData(data), written)
}
func (a printAd) SyncingFirstEntries(entsCid cid.Cid) {
	a.line("syncing first entries block %s", entsCid)
}
func (a printAd) HAMTAdOnly() { a.line("entries are HAMT, writing ad only") }
func (a printAd) WrittenFromPublisher(mainBroken, hamt bool, chunks, mhs int, written, downBytes int64) {
	action := publisherAction(mainBroken)
	if hamt {
		a.line("%s (HAMT skipped)  written=%d down_bytes=%d", action, written, downBytes)
		return
	}
	a.line("%s  chunks=%d mhs=%d written=%d down_bytes=%d", action, chunks, mhs, written, downBytes)
}
func (a printAd) SyncingRemaining(entsCid cid.Cid) {
	a.line("syncing remaining entry chunks from %s", entsCid)
}
