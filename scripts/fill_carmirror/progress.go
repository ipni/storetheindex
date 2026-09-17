package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Observer is notified at each interesting point in a fill. Method names say
// what happened; arguments carry every number and identity the backend needs
// to print or log. PrintProgress is the CLI default; LogProgress is used when
// --log is set. A nil Observer is a no-op.
type Observer interface {
	Start(opts Options)
	UsingIndexer(startAd cid.Cid, indexerURL string)
	Estimating(timeout time.Duration)
	CountedAds(n int)
	CountComplete(total int, exact bool)
	Periodic()
	Done(reason string, err error)

	CheckingMain(ad AdRef)
	MainInvalid(ad AdRef, err error)
	MainMiss(ad AdRef)
	MainReadError(ad AdRef, err error)
	CheckingExternal(ad AdRef, i int, loc string)
	ExternalMiss(ad AdRef, i int)
	ExternalReadError(ad AdRef, i int, err error)
	ExternalInvalid(ad AdRef, i int, err error)
	ExternalHit(ad AdRef, i int)
	Loaded(ad AdRef, src source, data *carData)
	NotInMirrorsFetching(ad AdRef, publisher peer.ID)
	FetchedAd(ad AdRef, advertisement schema.Advertisement)
	SkipIsRm(ad AdRef, advertisement schema.Advertisement, carOnMain bool)
	SkipNoEntries(ad AdRef, advertisement schema.Advertisement)
	PresentOnMain(ad AdRef, data *carData)
	CopiedFromExternal(ad AdRef, data *carData, written int64)
	SyncingFirstEntries(ad AdRef, entsCid cid.Cid)
	HAMTAdOnly(ad AdRef)
	FetchingEntryChunk(ad AdRef, n int, chunkCid cid.Cid)
	FetchedEntryChunk(ad AdRef, n int, chunkCid cid.Cid, mhs, chunkBytes int, downBytes int64)
	WritingCAR(ad AdRef, chunks int)
	WrittenFromPublisher(ad AdRef, hamt bool, chunks, mhs int, written, downBytes int64)
}

// AdRef is which advertisement in the walk an event is about.
type AdRef struct {
	N   int
	Cid cid.Cid
}

type nopObserver struct{}

func (nopObserver) Start(Options)                                            {}
func (nopObserver) UsingIndexer(cid.Cid, string)                             {}
func (nopObserver) Estimating(time.Duration)                                 {}
func (nopObserver) CountedAds(int)                                           {}
func (nopObserver) CountComplete(int, bool)                                  {}
func (nopObserver) Periodic()                                                {}
func (nopObserver) Done(string, error)                                       {}
func (nopObserver) CheckingMain(AdRef)                                       {}
func (nopObserver) MainInvalid(AdRef, error)                                 {}
func (nopObserver) MainMiss(AdRef)                                           {}
func (nopObserver) MainReadError(AdRef, error)                               {}
func (nopObserver) CheckingExternal(AdRef, int, string)                      {}
func (nopObserver) ExternalMiss(AdRef, int)                                  {}
func (nopObserver) ExternalReadError(AdRef, int, error)                      {}
func (nopObserver) ExternalInvalid(AdRef, int, error)                        {}
func (nopObserver) ExternalHit(AdRef, int)                                   {}
func (nopObserver) Loaded(AdRef, source, *carData)                           {}
func (nopObserver) NotInMirrorsFetching(AdRef, peer.ID)                      {}
func (nopObserver) FetchedAd(AdRef, schema.Advertisement)                    {}
func (nopObserver) SkipIsRm(AdRef, schema.Advertisement, bool)               {}
func (nopObserver) SkipNoEntries(AdRef, schema.Advertisement)                {}
func (nopObserver) PresentOnMain(AdRef, *carData)                            {}
func (nopObserver) CopiedFromExternal(AdRef, *carData, int64)                {}
func (nopObserver) SyncingFirstEntries(AdRef, cid.Cid)                       {}
func (nopObserver) HAMTAdOnly(AdRef)                                         {}
func (nopObserver) FetchingEntryChunk(AdRef, int, cid.Cid)                   {}
func (nopObserver) FetchedEntryChunk(AdRef, int, cid.Cid, int, int, int64)   {}
func (nopObserver) WritingCAR(AdRef, int)                                    {}
func (nopObserver) WrittenFromPublisher(AdRef, bool, int, int, int64, int64) {}

func observerOrNop(o Observer) Observer {
	if o == nil {
		return nopObserver{}
	}
	return o
}

// throttlePeriodic rate-limits Periodic calls to ticker; other methods pass through.
type throttlePeriodic struct {
	Observer
	ticker *time.Ticker
}

func (t *throttlePeriodic) Periodic() {
	select {
	case <-t.ticker.C:
		t.Observer.Periodic()
	default:
	}
}

type noPeriodic struct{ Observer }

func (noPeriodic) Periodic() {}

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

func formatScanned(scanned, total int, exact bool) string {
	if total <= 0 {
		return fmt.Sprintf("%d", scanned)
	}
	if exact {
		pct := 0
		if total > 0 {
			pct = scanned * 100 / total
		}
		return fmt.Sprintf("%d/%d (%d%%)", scanned, total, pct)
	}
	return fmt.Sprintf("%d/%d+", scanned, total)
}

// counts is accumulated by Observer implementations from fill events.
type counts struct {
	mu                                    sync.Mutex
	scanned, present, copied, downloaded  int
	skippedHAMT, skippedNoEnts, skippedRm int
	chunks, mhs                           int
	downBytes, written                    int64
	// lastAd is the furthest advertisement in walk order (N=1, 2, ...) that is
	// fully processed with no unfinished ads between it and the start. Safe
	// --cid to resume after interrupt: every newer ad already has a correct
	// CAR (or was skipped).
	lastAd   cid.Cid
	finished map[int]cid.Cid
	doneN    int
	total    int
	exact    bool
	stop     string

	startedAt  time.Time
	rateWindow []carRateSample
}

// carRateWindow is how many Periodic dumps back the recent cars/s rate spans.
const carRateWindow = 5

type carRateSample struct {
	at   time.Time
	cars int
}

func (c *counts) locked() func() {
	c.mu.Lock()
	return c.mu.Unlock
}

func (c *counts) noteChecking(ad AdRef) {
	c.scanned++
}

// noteFinished records that ad is done (CAR on main, or skip). lastAd advances
// through consecutive walk indexes until the first gap.
func (c *counts) noteFinished(ad AdRef) {
	if ad.N <= c.doneN {
		return
	}
	if c.finished == nil {
		c.finished = make(map[int]cid.Cid)
	}
	c.finished[ad.N] = ad.Cid
	for {
		next, ok := c.finished[c.doneN+1]
		if !ok {
			return
		}
		delete(c.finished, c.doneN+1)
		c.doneN++
		c.lastAd = next
	}
}

func (c *counts) notePresent(data *carData) {
	c.present++
	if data.hamt {
		c.skippedHAMT++
		return
	}
	c.chunks += data.chunks
	c.mhs += data.mhs
}

func (c *counts) noteCopied(data *carData, written int64) {
	c.copied++
	c.written += written
	if data.hamt {
		c.skippedHAMT++
		return
	}
	c.chunks += data.chunks
	c.mhs += data.mhs
}

func (c *counts) noteDownloaded(hamt bool, chunks, mhs int, written, downBytes int64) {
	c.downloaded++
	c.written += written
	c.downBytes = downBytes
	if hamt {
		c.skippedHAMT++
		return
	}
	c.chunks += chunks
	c.mhs += mhs
}

func (c *counts) noteSkipRm()     { c.skippedRm++ }
func (c *counts) noteSkipNoEnts() { c.skippedNoEnts++ }
func (c *counts) noteCountComplete(total int, exact bool) {
	c.total = total
	c.exact = exact
}
func (c *counts) noteDone(reason string) { c.stop = reason }

func (c *counts) noteStart() {
	c.startedAt = time.Now()
	c.rateWindow = []carRateSample{{at: c.startedAt, cars: 0}}
}

func (c *counts) notePeriodic() {
	c.rateWindow = append(c.rateWindow, carRateSample{at: time.Now(), cars: c.carsDone()})
	if extra := len(c.rateWindow) - carRateWindow; extra > 0 {
		c.rateWindow = c.rateWindow[extra:]
	}
}

func (c *counts) carsDone() int {
	return c.present + c.copied + c.downloaded
}

func carRate(from, to carRateSample) float64 {
	dt := to.at.Sub(from.at).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(to.cars-from.cars) / dt
}

func (c *counts) recentCarRate() float64 {
	if len(c.rateWindow) == 0 {
		return 0
	}

	return carRate(
		c.rateWindow[0],
		c.rateWindow[len(c.rateWindow)-1],
	)

}

func (c *counts) overallCarRate() float64 {
	if c.startedAt.IsZero() {
		return 0
	}

	return carRate(
		carRateSample{at: c.startedAt, cars: 0},
		carRateSample{at: time.Now(), cars: c.carsDone()},
	)
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
