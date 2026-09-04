package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

func TestPrintProgressTimestamps(t *testing.T) {
	var buf bytes.Buffer
	fixed := time.Date(2026, 9, 4, 9, 56, 1, 123000000, time.FixedZone("CEST", 2*3600))
	p := &PrintProgress{w: &buf, now: func() time.Time { return fixed }}

	p.Start(Options{})
	ap := p.Ad(1, cid.Undef, 0, false)
	ap.CheckingMain()
	p.Periodic(Stats{Scanned: 3})

	wantPrefix := "2026-09-04T09:56:01.123+0200  "
	out := buf.String()
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		if !strings.HasPrefix(line, wantPrefix) {
			t.Fatalf("line missing timestamp prefix %q: %q", wantPrefix, line)
		}
	}
	if !strings.Contains(out, "checking main") {
		t.Fatalf("missing checking main: %s", out)
	}
	if !strings.Contains(out, "[1] ") {
		t.Fatalf("missing ad index: %s", out)
	}
	if !strings.Contains(out, "progress  scanned=3") {
		t.Fatalf("missing progress: %s", out)
	}
}

func TestFormatAdIndex(t *testing.T) {
	if got := formatAdIndex(1, 0, false); got != "[1]" {
		t.Fatalf("no total: got %q", got)
	}
	if got := formatAdIndex(3, 10, true); got != "[3 / 10]" {
		t.Fatalf("exact: got %q", got)
	}
	if got := formatAdIndex(3, 10, false); got != "[3 / 10+]" {
		t.Fatalf("partial: got %q", got)
	}
}

func TestPrintAdIndexIncludesTotal(t *testing.T) {
	var buf bytes.Buffer
	p := &PrintProgress{w: &buf, now: func() time.Time { return time.Time{} }}
	p.Ad(3, cid.Undef, 10, true).CheckingMain()
	if !strings.Contains(buf.String(), "[3 / 10] ") {
		t.Fatalf("missing total in ad line: %s", buf.String())
	}
}

func TestPrintEntryChunkProgress(t *testing.T) {
	var buf bytes.Buffer
	p := &PrintProgress{w: &buf, now: func() time.Time { return time.Time{} }}
	ap := p.Ad(1, cid.Undef, 0, false)
	ap.FetchingEntryChunk(2, cid.Undef)
	ap.FetchedEntryChunk(2, cid.Undef, 16, 100, 500)
	out := buf.String()
	if !strings.Contains(out, "fetching entry chunk 2") {
		t.Fatalf("missing fetching: %s", out)
	}
	if !strings.Contains(out, "got entry chunk 2") || !strings.Contains(out, "mhs=16") {
		t.Fatalf("missing got: %s", out)
	}
}

func TestPrintCARStoreProgress(t *testing.T) {
	var buf bytes.Buffer
	p := &PrintProgress{w: &buf, now: func() time.Time { return time.Time{} }}
	ap := p.Ad(1, cid.Undef, 0, false)
	ap.WritingCAR(3)
	ap.StoringEntryChunk(2, 3, cid.Undef, 16, 100)
	ap.StoringCARFile()
	ap.StoringCARFileBytes(64)
	out := buf.String()
	if !strings.Contains(out, "writing CAR (3 chunks)") {
		t.Fatalf("missing writing: %s", out)
	}
	if !strings.Contains(out, "storing CAR chunk 2/3") {
		t.Fatalf("missing storing chunk: %s", out)
	}
	if !strings.Contains(out, "compressing and storing CAR file") {
		t.Fatalf("missing storing file: %s", out)
	}
	if !strings.Contains(out, "storing CAR file  bytes=64") {
		t.Fatalf("missing file bytes: %s", out)
	}
}

func TestFormatScanned(t *testing.T) {
	if got := formatScanned(Stats{Scanned: 3}); got != "3" {
		t.Fatalf("no total: got %q", got)
	}
	if got := formatScanned(Stats{Scanned: 3, TotalAds: 10, TotalExact: true}); got != "3/10 (30%)" {
		t.Fatalf("exact: got %q", got)
	}
	if got := formatScanned(Stats{Scanned: 3, TotalAds: 10}); got != "3/10+" {
		t.Fatalf("partial: got %q", got)
	}
}
