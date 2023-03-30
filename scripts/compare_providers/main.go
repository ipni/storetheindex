package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	findhttpclient "github.com/ipni/go-libipni/find/client/http"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

type compareProvidersStats struct {
	unknown              uint
	latestAdMismatch     uint
	latestAdMatch        uint
	totalSourceProviders uint
	totalTargetProviders uint
}

func (s compareProvidersStats) print() {
	fmt.Println("Stats:")
	fmt.Printf("  OK:                     %d\n", s.latestAdMatch)
	fmt.Printf("  Unknown by target:      %d\n", s.unknown)
	fmt.Printf("  Latest Ad Mismatch:     %d\n", s.latestAdMismatch)
	fmt.Println("  --------------------------")
	fmt.Printf("  Total source providers: %d\n", s.totalSourceProviders)
	fmt.Printf("  Total target providers: %d\n", s.totalTargetProviders)
}

func main() {
	source := flag.String("source", "", "Source indexer")
	target := flag.String("target", "", "Target indexer")

	flag.Parse()

	if *source == "" || *target == "" {
		log.Fatal("both indexer instances must be specified")
	}

	sourceClient, err := findhttpclient.New(*source)
	if err != nil {
		log.Fatal(err)
	}

	targetClient, err := findhttpclient.New(*target)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	sourceProvs, err := sourceClient.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	targetProvs, err := targetClient.ListProviders(ctx)
	if err != nil {
		log.Fatal(err)
	}

	targets := make(map[peer.ID]*model.ProviderInfo)
	for _, target := range targetProvs {
		if target.AddrInfo.ID == "" {
			continue
		}
		targets[target.AddrInfo.ID] = target
	}
	var stats compareProvidersStats
	for _, p := range sourceProvs {
		id := p.AddrInfo.ID
		fmt.Printf("%s: ", id)
		if other, exists := targets[id]; !exists {
			fmt.Println("Unknown by target indexer.")
			stats.unknown++
		} else if p.LastAdvertisement != other.LastAdvertisement {
			fmt.Println("Mismatching latest ad")
			// TODO implement diagnosis of which is ahead/behind and by how many ads.
			stats.latestAdMismatch++
		} else {
			fmt.Println("OK")
			stats.latestAdMatch++
		}
	}
	stats.totalSourceProviders = uint(len(sourceProvs))
	stats.totalTargetProviders = uint(len(targetProvs))
	fmt.Println()
	stats.print()
}
