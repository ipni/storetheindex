package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/gologshim"
)

const loggerName = "fill_carmirror"

// Discard until setupLogger wires slog through go-log, so tests stay quiet.
var log = slog.New(slog.DiscardHandler)

func setupLogger() error {
	cfg := logging.GetConfig()
	format, err := resolveLogFormat()
	if err != nil {
		return err
	}
	cfg.Format = format
	if !cfg.Stdout && !cfg.Stderr && cfg.File == "" && cfg.URL == "" {
		cfg.Stderr = true
	}
	logging.SetupLogging(cfg)
	if os.Getenv("GOLOG_LOG_LEVEL") == "" && os.Getenv("IPFS_LOGGING") == "" {
		if err := logging.SetLogLevel(loggerName, "info"); err != nil {
			return err
		}
	}

	handler := logging.SlogHandler()
	slog.SetDefault(slog.New(handler))
	gologshim.SetDefaultHandler(handler)
	log = slog.New(handler.WithAttrs([]slog.Attr{slog.String("logger", loggerName)}))
	return nil
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

func logStart(log *slog.Logger, opts Options) *slog.Logger {
	l := log.With(
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
	return l
}

func logStats(log *slog.Logger, s Stats) *slog.Logger {
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
	return l
}

func logAdFields(log *slog.Logger, ad schema.Advertisement) *slog.Logger {
	l := log.With("provider", ad.Provider, "isRm", ad.IsRm)
	if p := ad.PreviousCid(); p != cid.Undef {
		l = l.With("prev", p)
	}
	if hasEntries(ad) {
		l = l.With("entries", ad.Entries.(cidlink.Link).Cid)
	}
	return l
}

func logCarFields(log *slog.Logger, data *carData) *slog.Logger {
	if data == nil {
		return log
	}
	kind := "entries"
	if data.hamt {
		kind = "HAMT"
	} else if !hasEntries(data.ad) {
		kind = "no-entries"
	}
	return log.With("kind", kind, "chunks", data.chunks, "multihashes", data.mhs, "carBytes", data.size)
}
