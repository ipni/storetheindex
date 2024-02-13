package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/ipfs/go-cid"
	findclient "github.com/ipni/go-libipni/find/client"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
)

func main() {
	source := flag.String("source", "", "Source indexer")
	target := flag.String("target", "", "Target indexer")
	help := flag.Bool("help", false, "Print usage")

	flag.Parse()

	if *help {
		fmt.Print(`
Cross-announce announces all the providers from a given source indexer to a given target indexer.
Simply, specify the source and target as the HTTP(S) IPNI indexer instance. Example:
   $ go run ./scripts/cross_announce/main.go --source https://one-indexer.example --target https://another-indexer.example
`)
		return
	}

	if *source == "" || *target == "" {
		log.Fatal("both indexer instances must be specified")
	}

	sourcer, err := findclient.New(*source)
	if err != nil {
		log.Fatal(err)
	}

	targeter, err := ingestclient.New(*target)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	fmt.Printf("Listing providers at %s...\n", *source)
	providers, err := sourcer.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}
	totalProviders := len(providers)
	fmt.Printf("\tFound %d provider(s).\n", totalProviders)
	fmt.Printf("Announcing providers to %s...\n", *target)

	var announcedSuccessfully int
	for i, provider := range providers {
		fmt.Printf("\t(%d/%d) ", (i + 1), totalProviders)
		switch {
		case provider.Publisher == nil, provider.Publisher.ID == "", len(provider.Publisher.Addrs) == 0:
			fmt.Printf("No publisher for provider %s; skipped  announce.\n", provider.AddrInfo.ID)
			continue
		case cid.Undef.Equals(provider.LastAdvertisement):
			fmt.Printf("No last advertisement CID for provider %s; skipped  announce.\n", provider.AddrInfo.ID)
			continue
		//TODO: add more filtering like by last seen or lag
		default:
			if err := targeter.Announce(ctx, provider.Publisher, provider.LastAdvertisement); err != nil {
				fmt.Printf("Failed to announce provider %s: %s \n", provider.AddrInfo.ID, err)
				continue
			}
			fmt.Printf("Successfully announced provider %s\n", provider.AddrInfo.ID)
			announcedSuccessfully++
		}
	}
	fmt.Printf("Successfully announced %d out of %d discovered from source.", announcedSuccessfully, totalProviders)
}
