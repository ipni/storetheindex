package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/apierror"
	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
	"github.com/libp2p/go-libp2p/core/peer"
)

type options struct {
	source        string
	target        string
	targetFindURL string
	providerID    string
	timeout       time.Duration
	httpTimeout   time.Duration
	maxFailures   int
	dryRun        bool
}

// stats tracks the outcome of a cross-announce run.
//
// total is the number of providers returned by the source's ListProviders.
// Every source provider lands in exactly one of the provider-level buckets:
// skipped, deduped, or the head of a surviving publisher group. Each head
// then lands in exactly one of the head-level buckets: upToDate, notAllowed,
// failed, or announced.
type stats struct {
	total      int
	announced  int
	skipped    int
	deduped    int
	upToDate   int
	notAllowed int
	failed     int
}

func (s stats) print() {
	fmt.Printf("total:      %d\n", s.total)
	fmt.Printf("announced:  %d\n", s.announced)
	fmt.Printf("skipped:    %d\n", s.skipped)
	fmt.Printf("deduped:    %d\n", s.deduped)
	fmt.Printf("upToDate:   %d\n", s.upToDate)
	fmt.Printf("notAllowed: %d\n", s.notAllowed)
	fmt.Printf("failed:     %d\n", s.failed)
}

// publisherHead is the advertisement to announce for one publisher: the
// newest LastAdvertisement among the publisher's providers, with a
// deterministic tie-break on the lowest provider ID.
type publisherHead struct {
	publisher  *peer.AddrInfo
	lastAd     cid.Cid
	lastAdTime string
	provider   *model.ProviderInfo
}

// groupByPublisher groups the source providers by publisher and selects one
// head per group. Providers with no usable publisher or no defined
// LastAdvertisement are skipped. Non-winning group members are deduped.
//
// Every input provider lands in exactly one bucket: skipped, deduped, or the
// head of a surviving group, so skipped + deduped + len(heads) == len(provs).
func groupByPublisher(provs []*model.ProviderInfo) (heads map[peer.ID]publisherHead, skipped int, deduped int) {
	type entry struct {
		provider *model.ProviderInfo
		t        time.Time
		hasTime  bool
	}

	groups := make(map[peer.ID][]entry)
	for _, p := range provs {
		if p.Publisher == nil || p.Publisher.ID == "" || len(p.Publisher.Addrs) == 0 || cid.Undef.Equals(p.LastAdvertisement) {
			skipped++
			continue
		}
		var t time.Time
		hasTime := false
		if ts, err := time.Parse(time.RFC3339, p.LastAdvertisementTime); err == nil {
			t = ts
			hasTime = true
		}
		groups[p.Publisher.ID] = append(groups[p.Publisher.ID], entry{provider: p, t: t, hasTime: hasTime})
	}

	heads = make(map[peer.ID]publisherHead, len(groups))
	for id, entries := range groups {
		sort.Slice(entries, func(i, j int) bool {
			ei, ej := entries[i], entries[j]
			if ei.hasTime != ej.hasTime {
				return ei.hasTime
			}
			if ei.hasTime && ei.t != ej.t {
				return ei.t.After(ej.t)
			}
			return ei.provider.AddrInfo.ID.String() < ej.provider.AddrInfo.ID.String()
		})
		winner := entries[0]
		heads[id] = publisherHead{
			publisher:  winner.provider.Publisher,
			lastAd:     winner.provider.LastAdvertisement,
			lastAdTime: winner.provider.LastAdvertisementTime,
			provider:   winner.provider,
		}
		deduped += len(entries) - 1
	}
	return heads, skipped, deduped
}

// selectCandidates picks the publisher heads to announce. It is pure: no
// network calls, no logging. The source is grouped by publisher, narrowed by
// -pid if set, and compared head-to-head against the target. A head equal to
// the target's head for the same publisher is upToDate and dropped; every
// other head is a candidate.
func selectCandidates(source []*model.ProviderInfo, target []*model.ProviderInfo, o options) ([]publisherHead, stats) {
	var st stats
	st.total = len(source)

	sourceHeads, skipped, deduped := groupByPublisher(source)
	st.skipped = skipped
	st.deduped = deduped

	if o.providerID != "" {
		var keep peer.ID
		for _, p := range source {
			if p.AddrInfo.ID.String() == o.providerID && p.Publisher != nil && p.Publisher.ID != "" {
				keep = p.Publisher.ID
				break
			}
		}
		narrowed := make(map[peer.ID]publisherHead, len(sourceHeads))
		for id, h := range sourceHeads {
			if id == keep {
				narrowed[id] = h
			} else {
				st.skipped++
			}
		}
		sourceHeads = narrowed
	}

	targetHeads, _, _ := groupByPublisher(target)

	var candidates []publisherHead
	for id, h := range sourceHeads {
		if th, ok := targetHeads[id]; ok && th.lastAd.Equals(h.lastAd) {
			st.upToDate++
			continue
		}
		candidates = append(candidates, h)
	}
	return candidates, st
}

func run(ctx context.Context, o options) (stats, error) {
	var st stats

	sourcer, err := findclient.New(o.source, findclient.WithClient(&http.Client{Timeout: o.httpTimeout}))
	if err != nil {
		return st, fmt.Errorf("creating find client: %w", err)
	}

	targeter, err := ingestclient.New(o.target, ingestclient.WithClient(&http.Client{Timeout: o.httpTimeout}))
	if err != nil {
		return st, fmt.Errorf("creating ingest client: %w", err)
	}

	fmt.Printf("Listing providers at %s...\n", o.source)
	providers, err := sourcer.ListProviders(ctx)
	if err != nil {
		return st, fmt.Errorf("listing providers: %w", err)
	}

	var targetProviders []*model.ProviderInfo
	if o.targetFindURL != "" {
		targetSourcer, err := findclient.New(o.targetFindURL, findclient.WithClient(&http.Client{Timeout: o.httpTimeout}))
		if err != nil {
			return st, fmt.Errorf("creating target find client: %w", err)
		}
		fmt.Printf("Listing providers at %s...\n", o.targetFindURL)
		targetProviders, err = targetSourcer.ListProviders(ctx)
		if err != nil {
			return st, fmt.Errorf("listing target providers: %w", err)
		}
	}

	candidates, st := selectCandidates(providers, targetProviders, o)
	fmt.Printf("\tFound %d provider(s), %d candidate(s).\n", st.total, len(candidates))
	fmt.Printf("Announcing providers to %s...\n", o.target)

	for i, head := range candidates {
		if err := ctx.Err(); err != nil {
			return st, err
		}

		fmt.Printf("\t(%d/%d) ", i+1, len(candidates))
		if o.dryRun {
			fmt.Printf("[dry-run] Would announce provider %s (publisher %s, ad %s)\n",
				head.provider.AddrInfo.ID, head.publisher.ID, head.lastAd)
			st.announced++
			continue
		}
		if err := targeter.Announce(ctx, head.publisher, head.lastAd); err != nil {
			var apiErr *apierror.Error
			if errors.As(err, &apiErr) && apiErr.Status() == http.StatusForbidden {
				fmt.Printf("Provider %s not allowed by target policy; skipped.\n", head.provider.AddrInfo.ID)
				st.notAllowed++
			} else {
				fmt.Printf("Failed to announce provider %s: %s\n", head.provider.AddrInfo.ID, err)
				st.failed++
			}
			continue
		}
		fmt.Printf("Successfully announced provider %s\n", head.provider.AddrInfo.ID)
		st.announced++
	}
	return st, nil
}

func main() {
	source := flag.String("source", "", "Source indexer (find API, default port 3000)")
	target := flag.String("target", "", "Target indexer (ingest API, default port 3001)")
	targetFind := flag.String("target-find", "", "Target indexer find API (default port 3000); used to compare publisher heads against the target. Empty disables the comparison.")
	providerID := flag.String("pid", "", "Only announce the publisher of the provider with this peer ID")
	timeout := flag.Duration("timeout", 30*time.Minute, "Overall run timeout; 0 disables. A timed-out run exits non-zero so the Kubernetes Job is marked Failed; keeping this below the Job's activeDeadlineSeconds ensures the summary is printed before exit.")
	httpTimeout := flag.Duration("http-timeout", 30*time.Second, "Per-request HTTP timeout")
	maxFailures := flag.Int("max-failures", 0, "Exit non-zero when the failure count exceeds this value. With 0, a single failure fails the Job.")
	dryRun := flag.Bool("dry-run", false, "Log what would be announced without making any requests to the target")
	help := flag.Bool("help", false, "Print usage")

	flag.Parse()

	if *help {
		fmt.Print(`
cross-announce announces all the providers from a given source indexer to a given target indexer.
Specify the source and target as the HTTP(S) IPNI indexer instance. Example:
    $ cross-announce --source https://one-indexer.example --target https://another-indexer.example
`)
		flag.PrintDefaults()
		return
	}

	if *source == "" || *target == "" {
		fmt.Fprintln(os.Stderr, "both indexer instances must be specified")
		os.Exit(1)
	}

	o := options{
		source:        *source,
		target:        *target,
		targetFindURL: *targetFind,
		providerID:    *providerID,
		timeout:       *timeout,
		httpTimeout:   *httpTimeout,
		maxFailures:   *maxFailures,
		dryRun:        *dryRun,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if o.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.timeout)
		defer cancel()
	}

	st, err := run(ctx, o)
	st.print()

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if st.failed > o.maxFailures {
		os.Exit(1)
	}
}
