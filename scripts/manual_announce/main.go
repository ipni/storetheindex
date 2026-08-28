package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var addrs stringList
	target := flag.String("target", "", "Ingest / assigner HTTP base URL")
	peerIDStr := flag.String("peer", "", "Publisher peer ID")
	cidStr := flag.String("cid", "", "Advertisement CID to announce")
	flag.Var(&addrs, "addr", "Publisher multiaddr (repeatable)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Send a single HTTP announce to an indexer ingest API or assigner.

Usage:
  go run ./scripts/manual_announce --target URL --peer PEER_ID --cid CID --addr MULTIADDR [--addr MULTIADDR ...]

Example:
  go run ./scripts/manual_announce \
    --target https://cid.contact \
    --peer 12D3KooWP5UZNGnCPsCoCgxbc9BRDVwwgFguZ7EaW6FEMHTzN2q7 \
    --cid bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi \
    --addr /ip4/192.0.2.1/tcp/443/https

`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *target == "" || *peerIDStr == "" || *cidStr == "" || len(addrs) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	pid, err := peer.Decode(*peerIDStr)
	if err != nil {
		log.Fatalf("invalid peer id: %s", err)
	}
	adCid, err := cid.Decode(*cidStr)
	if err != nil {
		log.Fatalf("invalid cid: %s", err)
	}
	maddrs := make([]multiaddr.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Fatalf("invalid multiaddr %q: %s", addr, err)
		}
		maddrs = append(maddrs, ma)
	}

	icl, err := ingestclient.New(*target)
	if err != nil {
		log.Fatal(err)
	}
	err = icl.Announce(context.Background(), &peer.AddrInfo{
		ID:    pid,
		Addrs: maddrs,
	}, adCid)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Announced %s for peer %s to %s\n", adCid, pid, *target)
}
