package main

import (
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Progress receives fill events. PrintProgress is the CLI default; LogProgress
// is used when --log is set. A nil Progress is a no-op.
type Progress interface {
	Start(opts Options)
	UsingIndexer(startAd cid.Cid, indexerURL string)
	Estimating(timeout time.Duration)
	EstimateProgress(n int)
	Estimated(total int, exact bool)
	Ad(n int, adCid cid.Cid, total int, exact bool) AdProgress
	Periodic(s Stats)
	Done(s Stats, err error)
}

// AdProgress is Progress scoped to one advertisement in the walk.
type AdProgress interface {
	CheckingMain()
	MainInvalid(err error)
	MainMiss()
	MainReadError(err error)
	CheckingExternal(i int, loc string)
	ExternalMiss(i int)
	ExternalReadError(i int, err error)
	ExternalInvalid(i int, err error)
	ExternalHit(i int)
	Loaded(src source, data *carData)
	MainUnusableFetching(publisher peer.ID)
	NotInMirrorsFetching(publisher peer.ID)
	FetchedAd(ad schema.Advertisement)
	SkipIsRm(ad schema.Advertisement, carOnMain bool)
	SkipNoEntries(ad schema.Advertisement)
	PresentOnMain(data *carData)
	CopiedFromExternal(data *carData, written int64, recreated bool)
	SyncingFirstEntries(entsCid cid.Cid)
	HAMTAdOnly()
	FetchingEntryChunk(n int, chunkCid cid.Cid)
	FetchedEntryChunk(n int, chunkCid cid.Cid, mhs, chunkBytes int, downBytes int64)
	WrittenFromPublisher(mainBroken, hamt bool, chunks, mhs int, written, downBytes int64)
}

type nopProgress struct{}

func (nopProgress) Start(Options)                         {}
func (nopProgress) UsingIndexer(cid.Cid, string)          {}
func (nopProgress) Estimating(time.Duration)              {}
func (nopProgress) EstimateProgress(int)                  {}
func (nopProgress) Estimated(int, bool)                   {}
func (nopProgress) Ad(int, cid.Cid, int, bool) AdProgress { return nopAd{} }
func (nopProgress) Periodic(Stats)                        {}
func (nopProgress) Done(Stats, error)                     {}

type nopAd struct{}

func (nopAd) CheckingMain()                                           {}
func (nopAd) MainInvalid(error)                                       {}
func (nopAd) MainMiss()                                               {}
func (nopAd) MainReadError(error)                                     {}
func (nopAd) CheckingExternal(int, string)                            {}
func (nopAd) ExternalMiss(int)                                        {}
func (nopAd) ExternalReadError(int, error)                            {}
func (nopAd) ExternalInvalid(int, error)                              {}
func (nopAd) ExternalHit(int)                                         {}
func (nopAd) Loaded(source, *carData)                                 {}
func (nopAd) MainUnusableFetching(peer.ID)                            {}
func (nopAd) NotInMirrorsFetching(peer.ID)                            {}
func (nopAd) FetchedAd(schema.Advertisement)                          {}
func (nopAd) SkipIsRm(schema.Advertisement, bool)                     {}
func (nopAd) SkipNoEntries(schema.Advertisement)                      {}
func (nopAd) PresentOnMain(*carData)                                  {}
func (nopAd) CopiedFromExternal(*carData, int64, bool)                {}
func (nopAd) SyncingFirstEntries(cid.Cid)                             {}
func (nopAd) HAMTAdOnly()                                             {}
func (nopAd) FetchingEntryChunk(int, cid.Cid)                         {}
func (nopAd) FetchedEntryChunk(int, cid.Cid, int, int, int64)         {}
func (nopAd) WrittenFromPublisher(bool, bool, int, int, int64, int64) {}

func progressOrNop(p Progress) Progress {
	if p == nil {
		return nopProgress{}
	}
	return p
}

// throttlePeriodic rate-limits Periodic calls to ticker; other methods pass through.
type throttlePeriodic struct {
	Progress
	ticker *time.Ticker
}

func (t *throttlePeriodic) Periodic(s Stats) {
	select {
	case <-t.ticker.C:
		t.Progress.Periodic(s)
	default:
	}
}

type noPeriodic struct{ Progress }

func (noPeriodic) Periodic(Stats) {}

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

func formatAdIndex(n, total int, exact bool) string {
	if total <= 0 {
		return fmt.Sprintf("[%d]", n)
	}
	if exact {
		return fmt.Sprintf("[%d / %d]", n, total)
	}
	return fmt.Sprintf("[%d / %d+]", n, total)
}

func formatScanned(s Stats) string {
	if s.TotalAds <= 0 {
		return fmt.Sprintf("%d", s.Scanned)
	}
	if s.TotalExact {
		pct := 0
		if s.TotalAds > 0 {
			pct = s.Scanned * 100 / s.TotalAds
		}
		return fmt.Sprintf("%d/%d (%d%%)", s.Scanned, s.TotalAds, pct)
	}
	return fmt.Sprintf("%d/%d+", s.Scanned, s.TotalAds)
}

func carKind(data *carData) string {
	if data == nil {
		return ""
	}
	if data.hamt {
		return "HAMT"
	}
	if !hasEntries(data.ad) {
		return "no-entries"
	}
	return "entries"
}
