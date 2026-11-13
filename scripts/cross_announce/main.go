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
	"sync"
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
	source            string
	target            string
	targetFindURL     string
	providerID        string
	timeout           time.Duration
	httpTimeout       time.Duration
	maxFailures       int
	dryRun            bool
	skipInactive      bool
	skipLagging       bool
	lagFreshWithin    time.Duration
	allowNoTargetFind bool
	concurrency       int
}

// stats tracks the outcome of a cross-announce run.
//
// total is the number of providers returned by the source's ListProviders.
// Every source provider lands in exactly one of the provider-level buckets:
// skipped, deduped, or the head of a surviving publisher group. Each head
// then lands in exactly one of the head-level buckets: upToDate, inactive,
// lagging, targetAhead, unverifiable, notAllowed, failed, or announced.
type stats struct {
	total        int
	announced    int
	skipped      int
	deduped      int
	upToDate     int
	inactive     int
	lagging      int
	targetAhead  int
	unverifiable int
	notAllowed   int
	failed       int
}

func (s stats) print() {
	fmt.Printf("total:         %d\n", s.total)
	fmt.Printf("announced:     %d\n", s.announced)
	fmt.Printf("skipped:       %d\n", s.skipped)
	fmt.Printf("deduped:       %d\n", s.deduped)
	fmt.Printf("upToDate:      %d\n", s.upToDate)
	fmt.Printf("inactive:      %d\n", s.inactive)
	fmt.Printf("lagging:       %d\n", s.lagging)
	fmt.Printf("targetAhead:   %d\n", s.targetAhead)
	fmt.Printf("unverifiable:  %d\n", s.unverifiable)
	fmt.Printf("notAllowed:    %d\n", s.notAllowed)
	fmt.Printf("failed:        %d\n", s.failed)
}

// publisherHead is the advertisement to announce for one publisher: the
// newest LastAdvertisement among the publisher's providers, with a
// deterministic tie-break on the lowest provider ID.
//
// allInactive and maxLag are aggregated across the whole group because
// Inactive and Lag are per-provider; consulting only the winning provider
// would let one provider's state speak for the group.
type publisherHead struct {
	publisher   *peer.AddrInfo
	lastAd      cid.Cid
	lastAdTime  string
	provider    *model.ProviderInfo
	allInactive bool
	maxLag      int
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

		allInactive := true
		maxLag := 0
		for _, e := range entries {
			if !e.provider.Inactive {
				allInactive = false
			}
			if e.provider.Lag > maxLag {
				maxLag = e.provider.Lag
			}
		}

		heads[id] = publisherHead{
			publisher:   winner.provider.Publisher,
			lastAd:      winner.provider.LastAdvertisement,
			lastAdTime:  winner.provider.LastAdvertisementTime,
			provider:    winner.provider,
			allInactive: allInactive,
			maxLag:      maxLag,
		}
		deduped += len(entries) - 1
	}
	return heads, skipped, deduped
}

// selectCandidates picks the publisher heads to announce. It is pure: no
// network calls, no logging. The source is grouped by publisher, narrowed by
// -pid if set, and compared head-to-head against the target. A head equal to
// the target's head for the same publisher is upToDate and dropped; the
// safety guards then decide which of the remaining heads are safe to
// announce, and the rest become candidates.
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

	now := time.Now()
	var candidates []publisherHead
	for id, h := range sourceHeads {
		th, known := targetHeads[id]
		if known && th.lastAd.Equals(h.lastAd) {
			st.upToDate++
			continue
		}
		// Unknown to the target (absent, or registered but never ingested)
		// is a candidate, not a guard hit: these are the providers that most
		// need announcing.
		if !known || cid.Undef.Equals(th.lastAd) {
			candidates = append(candidates, h)
			continue
		}
		if o.skipInactive && h.allInactive {
			st.inactive++
			continue
		}
		if o.skipLagging && th.maxLag > 0 {
			if ts, err := time.Parse(time.RFC3339, th.lastAdTime); err == nil && now.Sub(ts) <= o.lagFreshWithin {
				st.lagging++
				continue
			}
		}
		// LastAdvertisementTime is the time each indexer received the
		// advertisement, not a property of the advertisement. The two
		// receive times proxy for chain position only while both indexers
		// are healthy and following the same publisher; during a backfill or
		// a stuck sync on one side the inference inverts. The guard is
		// therefore conservative.
		if stT, errS := time.Parse(time.RFC3339, h.lastAdTime); errS == nil {
			if ttT, errT := time.Parse(time.RFC3339, th.lastAdTime); errT == nil {
				if !ttT.Before(stT) {
					st.targetAhead++
					continue
				}
				candidates = append(candidates, h)
				continue
			}
		}
		st.unverifiable++
	}
	return candidates, st
}

// announceHead sends one head's advertisement to the target. It is a
// variable so a test can panic inside a worker: an httptest handler that
// panics is recovered by net/http before the client ever sees it, so no HTTP
// fixture can reach the worker's own recover path.
var announceHead = func(ctx context.Context, c *ingestclient.Client, head publisherHead) error {
	return c.Announce(ctx, head.publisher, head.lastAd)
}

// dispatchCandidates feeds the candidates to the workers and closes the
// channel on every exit path, including the early stop on cancellation;
// without the close the workers block on receive and the WaitGroup never
// completes, so a cancelled run would hang instead of returning a partial
// summary. The send is a select rather than a bare ctx.Err() check so that a
// cancellation which lands while every worker is busy stops the dispatcher
// too, instead of parking it on a send no worker will ever take.
func dispatchCandidates(ctx context.Context, ch chan publisherHead, candidates []publisherHead) {
	defer close(ch)
	for _, head := range candidates {
		select {
		case ch <- head:
		case <-ctx.Done():
			return
		}
	}
}

// printDryRun prints what would be announced, in the order the candidates
// are given, without making any request. It returns the number of
// candidates printed.
func printDryRun(ctx context.Context, candidates []publisherHead) (int, error) {
	announced := 0
	for _, head := range candidates {
		if err := ctx.Err(); err != nil {
			return announced, err
		}
		fmt.Printf("[dry-run] Would announce provider %s (publisher %s, ad %s)\n",
			head.provider.AddrInfo.ID, head.publisher.ID, head.lastAd)
		announced++
	}
	return announced, nil
}

func run(ctx context.Context, o options) (stats, error) {
	var st stats

	if o.targetFindURL == "" && !o.allowNoTargetFind {
		return st, errors.New("-target-find is required; set it to the target's find API or pass -allow-no-target-find to announce without comparing against the target")
	}

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

	if o.dryRun {
		// Dry-run performs no requests and starts no workers; it prints in
		// the order returned by selectCandidates, the only ordering
		// guarantee available, since the upstream /providers response order
		// is not stable.
		announced, err := printDryRun(ctx, candidates)
		st.announced += announced
		return st, err
	}

	// A bad cron argument degrades to serial rather than failing the run.
	concurrency := o.concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	type outcome struct {
		providerID peer.ID
		kind       int
		err        error
	}
	const (
		outAnnounced = iota
		outNotAllowed
		outFailed
	)

	// The candidate channel is sized to the worker count, not to the
	// candidate count. A buffer large enough to hold every candidate would
	// let the dispatcher hand off the whole set before a cancellation could
	// ever reach it, which makes its early exit, and the close that has to
	// accompany it, unreachable in production and untestable here.
	ch := make(chan publisherHead, concurrency)
	// One extra slot per worker so a panic report can always be sent without
	// blocking, even in the impossible case that every candidate has already
	// reported. A worker that blocks on this send never reaches wg.Done,
	// which is the hang this whole structure exists to avoid.
	results := make(chan outcome, len(candidates)+concurrency)

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			// The deferred function at the top of the goroutine body is the
			// backstop for every exit path: the channel close, the early
			// return on ctx.Done, and a panic that escapes the per-candidate
			// recover below. It must call wg.Done, and if a panic escaped it
			// must also report the failure. An unrecovered panic takes the
			// process down before stats.print runs and destroys the summary
			// for the whole run; a recover that skips either the send or
			// wg.Done turns that crash into a hang.
			defer func() {
				if r := recover(); r != nil {
					results <- outcome{kind: outFailed, err: fmt.Errorf("panic: %v", r)}
				}
				wg.Done()
			}()
			for {
				// Receiving in a select as well as checking ctx before each
				// announce means a cancelled worker can never be stranded on
				// an empty channel, independently of the dispatcher getting
				// its close right.
				var c publisherHead
				select {
				case v, ok := <-ch:
					if !ok {
						return
					}
					c = v
				case <-ctx.Done():
					return
				}
				// Cancellation during the previous announce must stop this
				// worker even if a candidate was ready to receive above.
				if ctx.Err() != nil {
					return
				}
				// The per-candidate recover keeps the worker alive after a
				// panic: a deferred recover on the goroutine body would end
				// the worker permanently, and with more candidates than
				// workers the pool would silently drop the rest of the run
				// and the dispatcher would block on a send with no live
				// receiver.
				func() {
					defer func() {
						if r := recover(); r != nil {
							results <- outcome{
								providerID: c.provider.AddrInfo.ID,
								kind:       outFailed,
								err:        fmt.Errorf("panic announcing publisher %s: %v", c.publisher.ID, r),
							}
						}
					}()
					if err := announceHead(ctx, targeter, c); err != nil {
						var apiErr *apierror.Error
						if errors.As(err, &apiErr) && apiErr.Status() == http.StatusForbidden {
							results <- outcome{providerID: c.provider.AddrInfo.ID, kind: outNotAllowed}
						} else {
							results <- outcome{providerID: c.provider.AddrInfo.ID, kind: outFailed, err: err}
						}
						return
					}
					results <- outcome{providerID: c.provider.AddrInfo.ID, kind: outAnnounced}
				}()
			}
		}()
	}

	go dispatchCandidates(ctx, ch, candidates)

	go func() {
		wg.Wait()
		close(results)
	}()

	// The collector is the only writer of the announce-phase counters, so
	// they need no mutex. Worker output interleaves, so the prefix is a
	// completed count rather than the candidate's index.
	var announced, notAllowed, failed, done int
	for res := range results {
		done++
		switch res.kind {
		case outAnnounced:
			fmt.Printf("\t(%d/%d) Successfully announced provider %s\n", done, len(candidates), res.providerID)
			announced++
		case outNotAllowed:
			fmt.Printf("\t(%d/%d) Provider %s not allowed by target policy; skipped.\n", done, len(candidates), res.providerID)
			notAllowed++
		case outFailed:
			fmt.Printf("\t(%d/%d) Failed to announce provider %s: %s\n", done, len(candidates), res.providerID, res.err)
			failed++
		}
	}

	// Merge the announce-phase counters into the selection counters once, at
	// the end.
	st.announced += announced
	st.notAllowed += notAllowed
	st.failed += failed

	if err := ctx.Err(); err != nil {
		return st, err
	}
	return st, nil
}

func main() {
	source := flag.String("source", "", "Source indexer (find API, default port 3000)")
	target := flag.String("target", "", "Target indexer (ingest API, default port 3001)")
	targetFind := flag.String("target-find", "", "Target indexer find API (default port 3000); used to compare publisher heads against the target. Required unless -allow-no-target-find is set.")
	providerID := flag.String("pid", "", "Only announce the publisher of the provider with this peer ID")
	timeout := flag.Duration("timeout", 30*time.Minute, "Overall run timeout; 0 disables. A timed-out run exits non-zero so the Kubernetes Job is marked Failed; keeping this below the Job's activeDeadlineSeconds ensures the summary is printed before exit.")
	httpTimeout := flag.Duration("http-timeout", 30*time.Second, "Per-request HTTP timeout")
	maxFailures := flag.Int("max-failures", 0, "Exit non-zero when the failure count exceeds this value. With 0, a single failure fails the Job.")
	dryRun := flag.Bool("dry-run", false, "Log what would be announced without making any requests to the target")
	skipInactive := flag.Bool("skip-inactive", true, "Drop source publisher groups where every provider is Inactive")
	skipLagging := flag.Bool("skip-lagging", false, "Drop candidates whose target group has a non-zero Lag with a fresh LastAdvertisementTime. Off by default because a stale Lag would otherwise skip a provider on every run.")
	lagFreshWithin := flag.Duration("lag-fresh-within", time.Hour, "A target LastAdvertisementTime within this window is fresh enough for -skip-lagging to act on a non-zero Lag")
	allowNoTargetFind := flag.Bool("allow-no-target-find", false, "Allow running without -target-find, which disables all safety guards and announces every candidate")
	concurrency := flag.Int("concurrency", 4, "Number of concurrent announce requests. This bounds concurrent advertisement chain syncs started on the target, not HTTP load. Values below 1 run serially.")
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
		source:            *source,
		target:            *target,
		targetFindURL:     *targetFind,
		providerID:        *providerID,
		timeout:           *timeout,
		httpTimeout:       *httpTimeout,
		maxFailures:       *maxFailures,
		dryRun:            *dryRun,
		skipInactive:      *skipInactive,
		skipLagging:       *skipLagging,
		lagFreshWithin:    *lagFreshWithin,
		allowNoTargetFind: *allowNoTargetFind,
		concurrency:       *concurrency,
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
