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
	ap := p.Ad(1, cid.Undef)
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
	if !strings.Contains(out, "progress  scanned=3") {
		t.Fatalf("missing progress: %s", out)
	}
}
