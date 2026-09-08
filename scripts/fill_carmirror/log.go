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
	counts
	log *slog.Logger
}

func NewLogProgress(log *slog.Logger) *LogProgress {
	return &LogProgress{log: log}
}

func (p *LogProgress) withRef(ad AdRef) *slog.Logger {
	l := p.log.With("n", ad.N, "ad", ad.Cid)
	if p.total > 0 {
		l = l.With("totalAds", p.total, "totalExact", p.exact)
	}
	return l
}

func (p *LogProgress) withAdvertisement(l *slog.Logger, ad schema.Advertisement) *slog.Logger {
	l = l.With("provider", ad.Provider, "isRm", ad.IsRm)
	if prev := ad.PreviousCid(); prev != cid.Undef {
		l = l.With("prev", prev)
	}
	if hasEntries(ad) {
		l = l.With("entries", ad.Entries.(cidlink.Link).Cid)
	}
	return l
}

func (p *LogProgress) withCar(l *slog.Logger, data *carData) *slog.Logger {
	if data == nil {
		return l
	}
	return l.With("kind", carKind(data), "chunks", data.chunks, "multihashes", data.mhs, "carBytes", data.size)
}

func (p *LogProgress) Start(opts Options) {
	defer p.locked()()
	p.noteStart()
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
	defer p.locked()()
	p.log.Info("using LastAdvertisement from indexer", "startAd", startAd, "indexer", indexerURL)
}

func (p *LogProgress) Estimating(timeout time.Duration) {
	defer p.locked()()
	if timeout > 0 {
		p.log.Info("counting advertisements", "timeout", timeout)
		return
	}
	p.log.Info("counting advertisements")
}

func (p *LogProgress) CountedAds(n int) {
	defer p.locked()()
	p.log.Info("count progress", "advertisements", n)
}

func (p *LogProgress) CountComplete(total int, exact bool) {
	defer p.locked()()
	p.noteCountComplete(total, exact)
	p.log.Info("count complete", "totalAds", total, "exact", exact)
}

func (p *LogProgress) Periodic() {
	defer p.locked()()
	p.notePeriodic()
	p.withCounts(p.log).With("carsPerSec", p.recentCarRate()).Info("progress")
}

func (p *LogProgress) Done(reason string, err error) {
	defer p.locked()()
	p.noteDone(reason)
	l := p.withCounts(p.log).With("carsPerSec", p.overallCarRate())
	if err != nil {
		l.Error("fill failed", "err", err)
		return
	}
	l.Info("fill complete")
}

func (p *LogProgress) CheckingMain(ad AdRef) {
	defer p.locked()()
	p.noteChecking(ad)
	p.withRef(ad).Debug("checking main")
}
func (p *LogProgress) MainInvalid(ad AdRef, err error) {
	defer p.locked()()
	p.withRef(ad).Info("main CAR invalid, will recreate", "err", err)
}
func (p *LogProgress) MainMiss(ad AdRef) {
	defer p.locked()()
	p.withRef(ad).Debug("main miss")
}
func (p *LogProgress) MainReadError(ad AdRef, err error) {
	defer p.locked()()
	p.withRef(ad).Info("main read error, will recreate", "err", err)
}
func (p *LogProgress) CheckingExternal(ad AdRef, i int, loc string) {
	defer p.locked()()
	p.withRef(ad).Debug("checking external", "external", i, "location", loc)
}
func (p *LogProgress) ExternalMiss(ad AdRef, i int) {
	defer p.locked()()
	p.withRef(ad).Debug("external miss", "external", i)
}
func (p *LogProgress) ExternalReadError(ad AdRef, i int, err error) {
	defer p.locked()()
	p.withRef(ad).Info("external read error", "external", i, "err", err)
}
func (p *LogProgress) ExternalInvalid(ad AdRef, i int, err error) {
	defer p.locked()()
	p.withRef(ad).Info("external invalid CAR", "external", i, "err", err)
}
func (p *LogProgress) ExternalHit(ad AdRef, i int) {
	defer p.locked()()
	p.withRef(ad).Info("external hit", "external", i)
}
func (p *LogProgress) Loaded(ad AdRef, src source, data *carData) {
	defer p.locked()()
	p.withCar(p.withRef(ad), data).With("source", src.String()).Info("loaded advertisement")
}
func (p *LogProgress) NotInMirrorsFetching(ad AdRef, publisher peer.ID) {
	defer p.locked()()
	p.withRef(ad).Info("not in mirrors, fetching from publisher", "publisher", publisher)
}
func (p *LogProgress) FetchedAd(ad AdRef, advertisement schema.Advertisement) {
	defer p.locked()()
	p.withAdvertisement(p.withRef(ad), advertisement).Info("fetched advertisement")
}
func (p *LogProgress) SkipIsRm(ad AdRef, advertisement schema.Advertisement, carOnMain bool) {
	defer p.locked()()
	p.noteSkipRm()
	p.noteFinished(ad)
	p.withAdvertisement(p.withRef(ad), advertisement).Info("skip IsRm", "carOnMain", carOnMain)
}
func (p *LogProgress) SkipNoEntries(ad AdRef, advertisement schema.Advertisement) {
	defer p.locked()()
	p.noteSkipNoEnts()
	p.noteFinished(ad)
	p.withAdvertisement(p.withRef(ad), advertisement).Info("skip no-entries")
}
func (p *LogProgress) PresentOnMain(ad AdRef, data *carData) {
	defer p.locked()()
	p.notePresent(data)
	p.noteFinished(ad)
	p.withCar(p.withRef(ad), data).Info("present on main")
}
func (p *LogProgress) CopiedFromExternal(ad AdRef, data *carData, written int64) {
	defer p.locked()()
	p.noteCopied(data, written)
	p.noteFinished(ad)
	p.withCar(p.withRef(ad), data).Info("copied from external", "written", written)
}
func (p *LogProgress) SyncingFirstEntries(ad AdRef, entsCid cid.Cid) {
	defer p.locked()()
	p.withRef(ad).Info("syncing first entries block", "entries", entsCid)
}
func (p *LogProgress) HAMTAdOnly(ad AdRef) {
	defer p.locked()()
	p.withRef(ad).Info("entries are HAMT, writing ad only")
}
func (p *LogProgress) FetchingEntryChunk(ad AdRef, n int, chunkCid cid.Cid) {
	defer p.locked()()
	p.withRef(ad).Info("fetching entry chunk", "chunk", n, "entries", chunkCid)
}
func (p *LogProgress) FetchedEntryChunk(ad AdRef, n int, chunkCid cid.Cid, mhs, chunkBytes int, downBytes int64) {
	defer p.locked()()
	p.withRef(ad).Info("got entry chunk", "chunk", n, "entries", chunkCid, "multihashes", mhs, "bytes", chunkBytes, "bytesDownloaded", downBytes)
}
func (p *LogProgress) WritingCAR(ad AdRef, chunks int) {
	defer p.locked()()
	p.withRef(ad).Info("writing CAR", "chunks", chunks)
}
func (p *LogProgress) WrittenFromPublisher(ad AdRef, hamt bool, chunks, mhs int, written, downBytes int64) {
	defer p.locked()()
	p.noteDownloaded(hamt, chunks, mhs, written, downBytes)
	p.noteFinished(ad)
	l := p.withRef(ad).With("written", written, "bytesDownloaded", downBytes)
	if hamt {
		l = l.With("hamt", true)
	} else {
		l = l.With("chunks", chunks, "multihashes", mhs)
	}
	l.Info("downloaded")
}
func (c *counts) withCounts(log *slog.Logger) *slog.Logger {
	l := log.With(
		"scanned", c.scanned,
		"alreadyPresent", c.present,
		"copiedExternal", c.copied,
		"downloaded", c.downloaded,
		"skippedHAMT", c.skippedHAMT,
		"skippedNoEnts", c.skippedNoEnts,
		"skippedRm", c.skippedRm,
		"entryChunks", c.chunks,
		"multihashes", c.mhs,
		"bytesDownloaded", c.downBytes,
		"bytesWritten", c.written,
	)
	if c.lastAd != cid.Undef {
		l = l.With("lastAd", c.lastAd)
	}
	if c.stop != "" {
		l = l.With("stopReason", c.stop)
	}
	if c.total > 0 {
		l = l.With("totalAds", c.total, "totalExact", c.exact)
	}
	return l
}
