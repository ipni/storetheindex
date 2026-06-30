package registry

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

const maxPhaseHistory = 10

// syncTracker holds the live, mutable sync state for a single publisher. It
// reflects progress for one main provider (ad.Provider) at a time; extended
// providers are not tracked. Updated by the ingester and serialized for the
// admin sync-status API. The mutex guards concurrent access between the
// ingester (writer) and API readers.
type syncTracker struct {
	mu sync.Mutex

	provider peer.ID

	scan       phaseState[scanRun]
	processing phaseState[processingRun]
	download   phaseState[downloadRun]

	// downloadArmed is set by BeginProcessing and cleared on the first
	// SetDownloading in that processing run, so entry downloads for multiple
	// ads share one download run.
	downloadArmed bool
}

type phaseState[T any] struct {
	current T
	history []T
}

type phaseRun struct {
	startTime time.Time
	endTime   time.Time
	errMsg    string
}

func (r phaseRun) started() bool {
	return !r.startTime.IsZero()
}

func (r phaseRun) ongoing() bool {
	return r.started() && r.endTime.IsZero()
}

type scanRun struct {
	phaseRun
	adsScanned int
	headAd     cid.Cid
	currentAd  cid.Cid
}

type processingRun struct {
	phaseRun
	adsProcessed int
	adsTotal     int
	currentAd    cid.Cid
	errorCount   int
}

type downloadRun struct {
	phaseRun
	bytesDownloaded     int64
	entryChunkCount     int
	chunkMultihashCount int64
	hamtMultihashCount  int64
}

func archivePhase[T any](ps *phaseState[T], started func(T) bool) {
	if !started(ps.current) {
		return
	}
	ps.history = prependCapped(ps.history, ps.current, maxPhaseHistory)
	var zero T
	ps.current = zero
}

func prependCapped[T any](hist []T, item T, cap int) []T {
	hist = append([]T{item}, hist...)
	if len(hist) > cap {
		hist = hist[:cap]
	}
	return hist
}

func endRun(pr *phaseRun, err error) {
	if !pr.ongoing() {
		return
	}
	pr.endTime = time.Now()
	if err != nil {
		pr.errMsg = err.Error()
	}
}

// MarshalJSON serializes per-phase sync stats with bounded history.
func (t *syncTracker) MarshalJSON() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := syncStatusJSON{}
	if t.provider != "" {
		out.Provider = t.provider.String()
	}
	out.Scan, out.ScanHistory = marshalScanPhase(t.scan)
	out.Processing, out.ProcessingHistory = marshalProcessingPhase(t.processing)
	out.Download, out.DownloadHistory = marshalDownloadPhase(t.download)
	return json.Marshal(out)
}

type syncStatusJSON struct {
	Provider          string              `json:",omitempty"`
	Scan              *scanRunJSON        `json:",omitempty"`
	ScanHistory       []scanRunJSON       `json:",omitempty"`
	Processing        *processingRunJSON  `json:",omitempty"`
	ProcessingHistory []processingRunJSON `json:",omitempty"`
	Download          *downloadRunJSON    `json:",omitempty"`
	DownloadHistory   []downloadRunJSON   `json:",omitempty"`
}

type scanRunJSON struct {
	StartTime  string `json:",omitempty"`
	EndTime    string `json:",omitempty"`
	Ongoing    bool   `json:",omitempty"`
	Elapsed    string `json:",omitempty"`
	Error      string `json:",omitempty"`
	AdsScanned int    `json:",omitempty"`
	HeadAd     string `json:",omitempty"`
	CurrentAd  string `json:",omitempty"`
}

type processingRunJSON struct {
	StartTime    string `json:",omitempty"`
	EndTime      string `json:",omitempty"`
	Ongoing      bool   `json:",omitempty"`
	Elapsed      string `json:",omitempty"`
	Error        string `json:",omitempty"`
	AdsProcessed int    `json:",omitempty"`
	AdsTotal     int    `json:",omitempty"`
	AdsLeft      int    `json:",omitempty"`
	CurrentAd    string `json:",omitempty"`
	ErrorCount   int    `json:",omitempty"`
}

type downloadRunJSON struct {
	StartTime           string `json:",omitempty"`
	EndTime             string `json:",omitempty"`
	Ongoing             bool   `json:",omitempty"`
	Elapsed             string `json:",omitempty"`
	Error               string `json:",omitempty"`
	BytesDownloaded     int64  `json:",omitempty"`
	EntryChunkCount     int    `json:",omitempty"`
	ChunkMultihashCount int64  `json:",omitzero"`
	HamtMultihashCount  int64  `json:",omitzero"`
	MultihashCount      int64  `json:",omitempty"`
}

func marshalScanPhase(ps phaseState[scanRun]) (*scanRunJSON, []scanRunJSON) {
	var current *scanRunJSON
	if ps.current.started() {
		r := marshalScanRun(ps.current)
		current = &r
	}
	history := marshalScanHistory(ps.history)
	return current, history
}

func marshalProcessingPhase(ps phaseState[processingRun]) (*processingRunJSON, []processingRunJSON) {
	var current *processingRunJSON
	if ps.current.started() {
		r := marshalProcessingRun(ps.current)
		current = &r
	}
	history := marshalProcessingHistory(ps.history)
	return current, history
}

func marshalDownloadPhase(ps phaseState[downloadRun]) (*downloadRunJSON, []downloadRunJSON) {
	var current *downloadRunJSON
	if ps.current.started() {
		r := marshalDownloadRun(ps.current)
		current = &r
	}
	history := marshalDownloadHistory(ps.history)
	return current, history
}

func marshalScanHistory(hist []scanRun) []scanRunJSON {
	if len(hist) == 0 {
		return nil
	}
	out := make([]scanRunJSON, len(hist))
	for i, h := range hist {
		out[i] = marshalScanRun(h)
	}
	return out
}

func marshalProcessingHistory(hist []processingRun) []processingRunJSON {
	if len(hist) == 0 {
		return nil
	}
	out := make([]processingRunJSON, len(hist))
	for i, h := range hist {
		out[i] = marshalProcessingRun(h)
	}
	return out
}

func marshalDownloadHistory(hist []downloadRun) []downloadRunJSON {
	if len(hist) == 0 {
		return nil
	}
	out := make([]downloadRunJSON, len(hist))
	for i, h := range hist {
		out[i] = marshalDownloadRun(h)
	}
	return out
}

func marshalPhaseRun(pr phaseRun) (start, end, elapsed, errMsg string, ongoing bool) {
	if !pr.started() {
		return
	}
	start = pr.startTime.Format(time.RFC3339)
	ongoing = pr.ongoing()
	if ongoing {
		elapsed = time.Since(pr.startTime).Round(time.Second).String()
	} else {
		end = pr.endTime.Format(time.RFC3339)
		elapsed = pr.endTime.Sub(pr.startTime).Round(time.Second).String()
	}
	errMsg = pr.errMsg
	return
}

func marshalScanRun(r scanRun) scanRunJSON {
	start, end, elapsed, errMsg, ongoing := marshalPhaseRun(r.phaseRun)
	out := scanRunJSON{
		StartTime:  start,
		EndTime:    end,
		Ongoing:    ongoing,
		Elapsed:    elapsed,
		Error:      errMsg,
		AdsScanned: r.adsScanned,
	}
	if r.headAd != cid.Undef {
		out.HeadAd = r.headAd.String()
	}
	if r.currentAd != cid.Undef {
		out.CurrentAd = r.currentAd.String()
	}
	return out
}

func marshalProcessingRun(r processingRun) processingRunJSON {
	start, end, elapsed, errMsg, ongoing := marshalPhaseRun(r.phaseRun)
	out := processingRunJSON{
		StartTime:    start,
		EndTime:      end,
		Ongoing:      ongoing,
		Elapsed:      elapsed,
		Error:        errMsg,
		AdsProcessed: r.adsProcessed,
		AdsTotal:     r.adsTotal,
		CurrentAd:    cidString(r.currentAd),
		ErrorCount:   r.errorCount,
	}
	if r.adsTotal > 0 && r.adsProcessed < r.adsTotal {
		out.AdsLeft = r.adsTotal - r.adsProcessed
	}
	return out
}

func marshalDownloadRun(r downloadRun) downloadRunJSON {
	start, end, elapsed, errMsg, ongoing := marshalPhaseRun(r.phaseRun)
	mhCount := r.chunkMultihashCount + r.hamtMultihashCount
	out := downloadRunJSON{
		StartTime:           start,
		EndTime:             end,
		Ongoing:             ongoing,
		Elapsed:             elapsed,
		Error:               errMsg,
		BytesDownloaded:     r.bytesDownloaded,
		EntryChunkCount:     r.entryChunkCount,
		ChunkMultihashCount: r.chunkMultihashCount,
		HamtMultihashCount:  r.hamtMultihashCount,
		MultihashCount:      mhCount,
	}
	return out
}

func cidString(c cid.Cid) string {
	if c == cid.Undef {
		return ""
	}
	return c.String()
}

// recordAdScanned records that adCid was scanned during the chain walk. prov is
// the ad's main provider (ad.Provider). It starts a new scan when not already
// scanning, else increments.
func (t *syncTracker) recordAdScanned(prov peer.ID, adCid cid.Cid) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.scan.current.ongoing() {
		archivePhase(&t.scan, func(r scanRun) bool { return r.started() })
		t.scan.current = scanRun{
			phaseRun:   phaseRun{startTime: time.Now()},
			adsScanned: 1,
			headAd:     adCid,
			currentAd:  adCid,
		}
		if prov != "" {
			t.provider = prov
		}
		return t.scan.current.adsScanned
	}
	t.scan.current.adsScanned++
	t.scan.current.currentAd = adCid
	if prov != "" {
		t.provider = prov
	}
	return t.scan.current.adsScanned
}

// endScan marks the current scan run finished and moves it to ScanHistory.
func (t *syncTracker) endScan(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	endRun(&t.scan.current.phaseRun, err)
	archivePhase(&t.scan, func(r scanRun) bool { return r.started() })
}

// BeginProcessing starts a new processing run with the given total number of
// advertisements to process.
func (t *syncTracker) BeginProcessing(total int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	archivePhase(&t.processing, func(r processingRun) bool { return r.started() })
	t.processing.current = processingRun{
		phaseRun: phaseRun{startTime: time.Now()},
		adsTotal: total,
	}
	t.downloadArmed = true
}

// EndProcessing marks the current processing run finished, archives it and any
// ongoing download run, and clears downloadArmed.
func (t *syncTracker) EndProcessing(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	endRun(&t.processing.current.phaseRun, err)
	archivePhase(&t.processing, func(r processingRun) bool { return r.started() })

	if t.download.current.started() {
		if t.download.current.ongoing() {
			endRun(&t.download.current.phaseRun, err)
		}
		archivePhase(&t.download, func(r downloadRun) bool { return r.started() })
	}
	t.downloadArmed = false
}

// SetCurrentAd records the advertisement currently being processed and how many
// have been processed so far.
func (t *syncTracker) SetCurrentAd(c cid.Cid, processed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.processing.current.currentAd = c
	t.processing.current.adsProcessed = processed
}

// SetDownloading marks that entry data is being downloaded for the current
// advertisement. The first call after BeginProcessing starts a new download
// run; later calls in the same processing run accumulate into that run.
func (t *syncTracker) SetDownloading() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.downloadArmed {
		return
	}
	archivePhase(&t.download, func(r downloadRun) bool { return r.started() })
	t.download.current = downloadRun{
		phaseRun: phaseRun{startTime: time.Now()},
	}
	t.downloadArmed = false
}

// AddChunk records an indexed EntryChunk: it increments the entry chunk count
// and adds the chunk's multihashes to the chunk-based multihash count.
func (t *syncTracker) AddChunk(mhCount int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.download.current.entryChunkCount++
	t.download.current.chunkMultihashCount += int64(mhCount)
}

// AddHamtMultihashes adds to the HAMT-based multihash count. HAMT entries are
// not chunked, so no entry chunk count is recorded.
func (t *syncTracker) AddHamtMultihashes(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.download.current.hamtMultihashCount += int64(n)
}

// AddBytes adds to the count of downloaded entry-block bytes.
func (t *syncTracker) AddBytes(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.download.current.bytesDownloaded += n
}

// IncError increments the count of advertisement ingest errors.
func (t *syncTracker) IncError() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.processing.current.errorCount++
}
