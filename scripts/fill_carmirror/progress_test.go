package main

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
)

func TestPrintProgressTimestamps(t *testing.T) {
	var buf bytes.Buffer
	p := NewPrintProgress(&buf)

	p.Start(Options{})
	p.CheckingMain(AdRef{N: 1, Cid: cid.Undef})
	p.Periodic()

	out := buf.String()
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		ts, _, ok := strings.Cut(line, "  ")
		if !ok {
			t.Fatalf("line missing timestamp: %q", line)
		}
		if _, err := time.Parse(printTimeFormat, ts); err != nil {
			t.Fatalf("timestamp %q: %v", ts, err)
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
	p := NewPrintProgress(&buf)
	p.CountComplete(10, true)
	p.CheckingMain(AdRef{N: 3, Cid: cid.Undef})
	if !strings.Contains(buf.String(), "[3 / 10] ") {
		t.Fatalf("missing total in ad line: %s", buf.String())
	}
}

func TestPrintEntryChunkProgress(t *testing.T) {
	var buf bytes.Buffer
	p := NewPrintProgress(&buf)
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
	p := NewPrintProgress(&buf)
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

func TestNoteFinishedAdvancesThroughGaps(t *testing.T) {
	cids := random.Cids(4)
	var c counts
	c.noteFinished(AdRef{N: 2, Cid: cids[1]})
	c.noteFinished(AdRef{N: 4, Cid: cids[3]})
	if c.lastAd != cid.Undef {
		t.Fatalf("gap at 1: lastAd=%s", c.lastAd)
	}
	c.noteFinished(AdRef{N: 1, Cid: cids[0]})
	if c.lastAd != cids[1] {
		t.Fatalf("contiguous through 2: lastAd=%s want %s", c.lastAd, cids[1])
	}
	if c.doneN != 2 {
		t.Fatalf("doneN=%d want 2", c.doneN)
	}
	if _, ok := c.finished[4]; !ok {
		t.Fatal("expected unfinished gap to keep ad 4")
	}
	c.noteFinished(AdRef{N: 3, Cid: cids[2]})
	if c.lastAd != cids[3] {
		t.Fatalf("contiguous through 4: lastAd=%s want %s", c.lastAd, cids[3])
	}
	if len(c.finished) != 0 {
		t.Fatalf("finished leftovers: %v", c.finished)
	}
}

func TestCarRateRecentWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c counts
		c.noteStart()

		c.present = 100
		time.Sleep(10 * time.Second)
		c.notePeriodic()
		got := c.recentCarRate()
		if got != 10 {
			t.Fatalf("first dump from start: got %v want 10", got)
		}

		c.present = 110
		time.Sleep(time.Second)
		c.notePeriodic()
		got = c.recentCarRate()
		if got != 10 {
			t.Fatalf("second dump last interval: got %v want 10", got)
		}

		for i := 0; i < carRateWindow; i++ {
			time.Sleep(time.Second)
			c.notePeriodic()
			c.recentCarRate()
		}
		c.present = 110 + carRateWindow*1000
		time.Sleep(time.Second)
		c.notePeriodic()
		got = c.recentCarRate()
		want := float64(carRateWindow*1000) / float64(carRateWindow-1)
		if got != want {
			t.Fatalf("windowed dump: got %v want %v (overall would be much lower)", got, want)
		}
		overall := c.overallCarRate()
		if overall >= got {
			t.Fatalf("overall %v should be below recent %v", overall, got)
		}
	})
}

func TestPrintCarRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		p := NewPrintProgress(&buf)
		p.Start(Options{})
		p.PresentOnMain(AdRef{N: 1, Cid: cid.Undef}, &carData{})
		time.Sleep(2 * time.Second)
		p.Periodic()
		p.Done(stopGenesis, nil)
		out := buf.String()
		if !strings.Contains(out, "cars_per_sec=0.5") {
			t.Fatalf("missing periodic rate: %s", out)
		}
		if !strings.Contains(out, "cars per second:   0.5") {
			t.Fatalf("missing overall rate: %s", out)
		}
	})
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
