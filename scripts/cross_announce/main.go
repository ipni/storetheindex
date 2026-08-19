package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/apierror"
	findclient "github.com/ipni/go-libipni/find/client"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
)

type options struct {
	source      string
	target      string
	providerID  string
	timeout     time.Duration
	httpTimeout time.Duration
	maxFailures int
	dryRun      bool
}

// stats tracks the outcome of a cross-announce run.
//
// total is the number of providers returned by the source's ListProviders.
type stats struct {
	total      int
	announced  int
	skipped    int
	notAllowed int
	failed     int
}

func (s stats) print() {
	fmt.Printf("total:      %d\n", s.total)
	fmt.Printf("announced:  %d\n", s.announced)
	fmt.Printf("skipped:    %d\n", s.skipped)
	fmt.Printf("notAllowed: %d\n", s.notAllowed)
	fmt.Printf("failed:     %d\n", s.failed)
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
	st.total = len(providers)
	fmt.Printf("\tFound %d provider(s).\n", st.total)
	fmt.Printf("Announcing providers to %s...\n", o.target)

	for i, provider := range providers {
		if err := ctx.Err(); err != nil {
			return st, err
		}

		if o.providerID != "" && provider.AddrInfo.ID.String() != o.providerID {
			continue
		}

		fmt.Printf("\t(%d/%d) ", i+1, st.total)
		switch {
		case provider.Publisher == nil, provider.Publisher.ID == "", len(provider.Publisher.Addrs) == 0:
			fmt.Printf("No publisher for provider %s; skipped announce.\n", provider.AddrInfo.ID)
			st.skipped++
			continue
		case cid.Undef.Equals(provider.LastAdvertisement):
			fmt.Printf("No last advertisement CID for provider %s; skipped announce.\n", provider.AddrInfo.ID)
			st.skipped++
			continue
		default:
			if o.dryRun {
				fmt.Printf("[dry-run] Would announce provider %s (publisher %s, ad %s)\n",
					provider.AddrInfo.ID, provider.Publisher.ID, provider.LastAdvertisement)
				st.announced++
				continue
			}
			if err := targeter.Announce(ctx, provider.Publisher, provider.LastAdvertisement); err != nil {
				var apiErr *apierror.Error
				if errors.As(err, &apiErr) && apiErr.Status() == http.StatusForbidden {
					fmt.Printf("Provider %s not allowed by target policy; skipped.\n", provider.AddrInfo.ID)
					st.notAllowed++
				} else {
					fmt.Printf("Failed to announce provider %s: %s\n", provider.AddrInfo.ID, err)
					st.failed++
				}
				continue
			}
			fmt.Printf("Successfully announced provider %s\n", provider.AddrInfo.ID)
			st.announced++
		}
	}
	return st, nil
}

func main() {
	source := flag.String("source", "", "Source indexer (find API, default port 3000)")
	target := flag.String("target", "", "Target indexer (ingest API, default port 3001)")
	providerID := flag.String("pid", "", "Only announce the provider with this peer ID")
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
		source:      *source,
		target:      *target,
		providerID:  *providerID,
		timeout:     *timeout,
		httpTimeout: *httpTimeout,
		maxFailures: *maxFailures,
		dryRun:      *dryRun,
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
