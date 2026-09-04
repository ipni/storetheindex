package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/gologshim"
)

const loggerName = "fill_carmirror"

func setupLogger() (*slog.Logger, error) {
	cfg := logging.GetConfig()
	format, err := resolveLogFormat()
	if err != nil {
		return nil, err
	}
	cfg.Format = format
	if !cfg.Stdout && !cfg.Stderr && cfg.File == "" && cfg.URL == "" {
		cfg.Stderr = true
	}
	logging.SetupLogging(cfg)
	if os.Getenv("GOLOG_LOG_LEVEL") == "" && os.Getenv("IPFS_LOGGING") == "" {
		if err := logging.SetLogLevel(loggerName, "info"); err != nil {
			return nil, err
		}
	}

	handler := logging.SlogHandler()
	slog.SetDefault(slog.New(handler))
	gologshim.SetDefaultHandler(handler)
	return slog.New(handler.WithAttrs([]slog.Attr{slog.String("logger", loggerName)})), nil
}

func resolveLogFormat() (logging.LogFormat, error) {
	if v := os.Getenv("GOLOG_LOG_FMT"); v != "" {
		return parseLogFormat(v)
	}
	if v := os.Getenv("IPFS_LOGGING_FMT"); v != "" {
		return parseLogFormat(v)
	}
	return logging.JSONOutput, nil
}

func parseLogFormat(format string) (logging.LogFormat, error) {
	switch strings.ToLower(format) {
	case "json":
		return logging.JSONOutput, nil
	case "color", "text", "console":
		return logging.ColorizedOutput, nil
	case "nocolor", "plain":
		return logging.PlaintextOutput, nil
	default:
		return 0, fmt.Errorf("unknown log format %q (json, color, nocolor)", format)
	}
}

// LogProgress writes fill events as structured logs.
type LogProgress struct {
	log *slog.Logger
}

func NewLogProgress(log *slog.Logger) *LogProgress {
	return &LogProgress{log: log}
}

func (p *LogProgress) Start(opts Options) {
	l := p.log.With(
		"provider", opts.Provider,
		"mainMode", opts.Mirror.MainMode,
		"mainMirror", opts.Mirror.Main.Local.BasePath,
	)
	if opts.StartAd != cid.Undef {
		l = l.With("startAd", opts.StartAd)
	}
	if opts.Publisher.ID != "" {
		addrs := make([]string, len(opts.Publisher.Addrs))
		for i, a := range opts.Publisher.Addrs {
			addrs[i] = a.String()
		}
		l = l.With("publisher", opts.Publisher.ID, "publisherAddrs", addrs)
	}
	l.Info("starting fill")
	for i, ext := range opts.Mirror.External {
		loc := ext.HTTP.BaseURL
		if loc == "" {
			loc = ext.Local.BasePath
		}
		p.log.Info("external mirror", "index", i, "type", ext.Type, "location", loc)
	}
}

func (p *LogProgress) UsingIndexer(startAd cid.Cid, indexerURL string) {
	p.log.Info("using LastAdvertisement from indexer", "startAd", startAd, "indexer", indexerURL)
}

func (p *LogProgress) Estimating(timeout time.Duration) {
	if timeout > 0 {
		p.log.Info("counting advertisements", "timeout", timeout)
		return
	}
	p.log.Info("counting advertisements")
}

func (p *LogProgress) EstimateProgress(n int) {
	p.log.Info("count progress", "advertisements", n)
}

func (p *LogProgress) Estimated(total int, exact bool) {
	p.log.Info("count complete", "totalAds", total, "exact", exact)
}

func (p *LogProgress) Ad(n int, adCid cid.Cid, total int, exact bool) AdProgress {
	l := p.log.With("n", n, "ad", adCid)
	if total > 0 {
		l = l.With("totalAds", total, "totalExact", exact)
	}
	return logAd{log: l}
}

func (p *LogProgress) Periodic(s Stats) {
	withStats(p.log, s).Info("progress")
}

func (p *LogProgress) Done(s Stats, err error) {
	l := withStats(p.log, s)
	if err != nil {
		l.Error("fill failed", "err", err)
		return
	}
	l.Info("fill complete")
}

type logAd struct {
	log *slog.Logger
}

func (a logAd) withAd(ad schema.Advertisement) *slog.Logger {
	l := a.log.With("provider", ad.Provider, "isRm", ad.IsRm)
	if p := ad.PreviousCid(); p != cid.Undef {
		l = l.With("prev", p)
	}
	if hasEntries(ad) {
		l = l.With("entries", ad.Entries.(cidlink.Link).Cid)
	}
	return l
}

func (a logAd) withCar(data *carData) *slog.Logger {
	if data == nil {
		return a.log
	}
	return a.log.With("kind", carKind(data), "chunks", data.chunks, "multihashes", data.mhs, "carBytes", data.size)
}

func (a logAd) CheckingMain() { a.log.Debug("checking main") }
func (a logAd) MainInvalid(err error) {
	a.log.Info("main CAR invalid, will recreate", "err", err)
}
func (a logAd) MainMiss() { a.log.Debug("main miss") }
func (a logAd) MainReadError(err error) {
	a.log.Info("main read error, will recreate", "err", err)
}
func (a logAd) CheckingExternal(i int, loc string) {
	a.log.Debug("checking external", "external", i, "location", loc)
}
func (a logAd) ExternalMiss(i int) {
	a.log.Debug("external miss", "external", i)
}
func (a logAd) ExternalReadError(i int, err error) {
	a.log.Info("external read error", "external", i, "err", err)
}
func (a logAd) ExternalInvalid(i int, err error) {
	a.log.Info("external invalid CAR", "external", i, "err", err)
}
func (a logAd) ExternalHit(i int) { a.log.Info("external hit", "external", i) }
func (a logAd) Loaded(src source, data *carData) {
	a.withCar(data).With("source", src.String()).Info("loaded advertisement")
}
func (a logAd) MainUnusableFetching(publisher peer.ID) {
	a.log.Info("main CAR unusable, fetching from publisher", "publisher", publisher)
}
func (a logAd) NotInMirrorsFetching(publisher peer.ID) {
	a.log.Info("not in mirrors, fetching from publisher", "publisher", publisher)
}
func (a logAd) FetchedAd(ad schema.Advertisement) {
	a.withAd(ad).Info("fetched advertisement")
}
func (a logAd) SkipIsRm(ad schema.Advertisement, carOnMain bool) {
	a.withAd(ad).Info("skip IsRm", "carOnMain", carOnMain)
}
func (a logAd) SkipNoEntries(ad schema.Advertisement) {
	a.withAd(ad).Info("skip no-entries")
}
func (a logAd) PresentOnMain(data *carData) {
	a.withCar(data).Info("present on main")
}
func (a logAd) CopiedFromExternal(data *carData, written int64, recreated bool) {
	msg := "copied from external"
	if recreated {
		msg = "recreated from external"
	}
	a.withCar(data).Info(msg, "written", written)
}
func (a logAd) SyncingFirstEntries(entsCid cid.Cid) {
	a.log.Info("syncing first entries block", "entries", entsCid)
}
func (a logAd) HAMTAdOnly() { a.log.Info("entries are HAMT, writing ad only") }
func (a logAd) FetchingEntryChunk(n int, chunkCid cid.Cid) {
	a.log.Info("fetching entry chunk", "chunk", n, "entries", chunkCid)
}
func (a logAd) FetchedEntryChunk(n int, chunkCid cid.Cid, mhs, chunkBytes int, downBytes int64) {
	a.log.Info("got entry chunk", "chunk", n, "entries", chunkCid, "multihashes", mhs, "bytes", chunkBytes, "bytesDownloaded", downBytes)
}
func (a logAd) WritingCAR(chunks int) {
	a.log.Info("writing CAR", "chunks", chunks)
}
func (a logAd) StoringEntryChunk(n, total int, chunkCid cid.Cid, mhs, chunkBytes int) {
	a.log.Info("storing CAR chunk", "chunk", n, "total", total, "entries", chunkCid, "multihashes", mhs, "bytes", chunkBytes)
}
func (a logAd) StoringCARFile() {
	a.log.Info("compressing and storing CAR file")
}
func (a logAd) StoringCARFileBytes(n int64) {
	a.log.Info("storing CAR file", "bytes", n)
}
func (a logAd) WrittenFromPublisher(mainBroken, hamt bool, chunks, mhs int, written, downBytes int64) {
	l := a.log.With("written", written, "bytesDownloaded", downBytes)
	if hamt {
		l = l.With("hamt", true)
	} else {
		l = l.With("chunks", chunks, "multihashes", mhs)
	}
	l.Info(publisherAction(mainBroken))
}

func withStats(log *slog.Logger, s Stats) *slog.Logger {
	l := log.With(
		"scanned", s.Scanned,
		"alreadyPresent", s.AlreadyPresent,
		"copiedExternal", s.CopiedExternal,
		"downloaded", s.Downloaded,
		"recreated", s.Recreated,
		"skippedHAMT", s.SkippedHAMT,
		"skippedNoEnts", s.SkippedNoEnts,
		"skippedRm", s.SkippedRm,
		"entryChunks", s.EntryChunks,
		"multihashes", s.Multihashes,
		"bytesDownloaded", s.BytesDownloaded,
		"bytesWritten", s.BytesWritten,
	)
	if s.LastAd != cid.Undef {
		l = l.With("lastAd", s.LastAd)
	}
	if s.StopReason != "" {
		l = l.With("stopReason", s.StopReason)
	}
	if s.TotalAds > 0 {
		l = l.With("totalAds", s.TotalAds, "totalExact", s.TotalExact)
	}
	return l
}
