package main

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

func TestPrintProgressTimestamps(t *testing.T) {
	var buf bytes.Buffer
	fixed := time.Date(2026, 9, 4, 9, 56, 1, 123000000, time.FixedZone("CEST", 2*3600))
	p := &PrintProgress{w: &buf, now: func() time.Time { return fixed }}

	p.Start(Options{})
	p.CheckingMain(AdRef{N: 1, Cid: cid.Undef})
	p.Periodic()

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
	if !strings.Contains(out, "progress  scanned=1") {
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
	p.CountComplete(10, true)
	p.CheckingMain(AdRef{N: 3, Cid: cid.Undef})
	if !strings.Contains(buf.String(), "[3 / 10] ") {
		t.Fatalf("missing total in ad line: %s", buf.String())
	}
}

func TestPrintEntryChunkProgress(t *testing.T) {
	var buf bytes.Buffer
	p := &PrintProgress{w: &buf, now: func() time.Time { return time.Time{} }}
	ad := AdRef{N: 1, Cid: cid.Undef}
	p.FetchingEntryChunk(ad, 2, cid.Undef)
	p.FetchedEntryChunk(ad, 2, cid.Undef, 16, 100, 500)
	out := buf.String()
	if !strings.Contains(out, "fetching entry chunk 2") {
		t.Fatalf("missing fetching: %s", out)
	}
	if !strings.Contains(out, "got entry chunk 2") || !strings.Contains(out, "mhs=16") {
		t.Fatalf("missing got: %s", out)
	}
}

func TestPrintCARWriteProgress(t *testing.T) {
	var buf bytes.Buffer
	p := &PrintProgress{w: &buf, now: func() time.Time { return time.Time{} }}
	ad := AdRef{N: 1, Cid: cid.Undef}
	p.WritingCAR(ad, 3)
	p.WritingCAR(ad, 0)
	out := buf.String()
	if !strings.Contains(out, "writing CAR (3 chunks)") {
		t.Fatalf("missing writing: %s", out)
	}
	if !strings.Contains(out, "writing CAR (ad only)") {
		t.Fatalf("missing ad-only: %s", out)
	}
}

func TestFormatScanned(t *testing.T) {
	if got := formatScanned(3, 0, false); got != "3" {
		t.Fatalf("no total: got %q", got)
	}
	if got := formatScanned(3, 10, true); got != "3/10 (30%)" {
		t.Fatalf("exact: got %q", got)
	}
	if got := formatScanned(3, 10, false); got != "3/10+" {
		t.Fatalf("partial: got %q", got)
	}
}

func TestPrintProgressConcurrent(t *testing.T) {
	p := NewPrintProgress(io.Discard)
	var wg sync.WaitGroup
	const n = 32
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ad := AdRef{N: i + 1, Cid: cid.Undef}
			p.CheckingMain(ad)
			p.PresentOnMain(ad, &carData{})
			p.Periodic()
		}(i)
	}
	wg.Wait()
	p.Done(stopGenesis, nil)
	if p.scanned != n || p.present != n {
		t.Fatalf("scanned=%d present=%d want %d", p.scanned, p.present, n)
	}
}
