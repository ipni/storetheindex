package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	finderhttpclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
)

// This program can be used to reconcile results from regular and double hashed clients.
// It works as following:
// * fetch list of providers from source and target indexers
// * find providers that exist in both indexers and that have the same last ad cid
// * for each provider fetch last add via provider CLI
// * sample over [mhlimit] random multihashes using both clients and compare results
//
// The program requires provider CLI being installed and available on the $PATH
func main() {
	source := flag.String("source", "", "Source indexer")
	itarget := flag.String("itarget", "", "Target indexer for multihash lookups")
	ptarget := flag.String("ptarget", "", "Target indexer for provider lookups")
	mhlimit := flag.Int("mhlimit", 10, "Number of multihashes to sample from each provider")

	flag.Parse()

	if *source == "" || *itarget == "" || *ptarget == "" {
		log.Fatal("both indexer instances must be specified")
	}

	sourceClient, err := finderhttpclient.New(*source)
	if err != nil {
		log.Fatal(err)
	}

	targetClient, err := finderhttpclient.NewDHashClient(*itarget, *ptarget)
	if err != nil {
		log.Fatal(err)
	}

	sourceProvs, err := sourceClient.ListProviders(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	targetProvs, err := targetClient.ListProviders(context.Background())
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
	match := []*model.ProviderInfo{}
	for _, p := range sourceProvs {
		if other, exists := targets[p.AddrInfo.ID]; !exists || p.LastAdvertisement != other.LastAdvertisement {
			continue
		}
		match = append(match, p)
	}

	log.Printf("found %d matching providers", len(match))
	notMatched := 0
	matched := 0
	ctx := context.Background()

	for _, p := range match {
		adCid := p.LastAdvertisement.String()
		addr := p.AddrInfo.Addrs[0].String()
		pid := p.AddrInfo.ID.String()

		out, err := runCmd(ctx, "provider", "ls", "ad", "--print-entries", fmt.Sprintf("--provider-addr-info=%s/p2p/%s", addr, pid), adCid)
		if err != nil {
			log.Printf("error listing ad=%v, output=%s", err, string(out))
			continue
		}

		ai, err := getAdInfo(ctx, pid, addr, adCid)
		if err != nil {
			log.Printf("error fetching ad %s from provider %s: %v", adCid, pid, err)
			continue
		}
		// fetch previous ad as the latest might have been not fully indexed yet
		for {
			if len(ai.mhs) > 0 {
				break
			}
			if len(ai.prevCid) == 0 {
				break
			}
			adCid = ai.prevCid

			ai, err = getAdInfo(ctx, pid, addr, adCid)
			if err != nil {
				log.Printf("error fetching ad %s from provider %s: %v", adCid, pid, err)
				break
			}
		}
		if ai == nil || len(ai.mhs) == 0 {
			continue
		}

		log.Printf("processing provider=%s, ad=%s", pid, adCid)

		for i := 0; i < *mhlimit; i++ {
			mh := ai.mhs[rand.Intn(len(ai.mhs))]
			res1, err := sourceClient.Find(context.Background(), mh)
			if err != nil {
				log.Printf("\t[%s] error querying source client=%v", mh.B58String(), err)
				continue
			}
			res2, err := targetClient.Find(context.Background(), mh)
			if err != nil {
				log.Printf("\t[%s] error querying target client: %v", mh.B58String(), err)
				continue
			}
			if len(res1.MultihashResults) != 1 || len(res2.MultihashResults) != 1 {
				log.Printf("\t[%s] no match: mismatching multihash results length", mh.B58String())
				notMatched++
				continue
			}

			if m, errs := compare(res1.MultihashResults[0], res2.MultihashResults[0]); !m {
				log.Printf("\t[%s] no match: %s", mh.B58String(), strings.Join(errs, "; "))
				notMatched++
				continue
			}
			log.Printf("\t[%s] match", mh.B58String())
			matched++
		}
	}

	fmt.Println()
	fmt.Print("Stats\n")
	fmt.Printf("\tMatched: %d\n", matched)
	fmt.Printf("\tNot matched: %d", notMatched)
}

type adInfo struct {
	prevCid string
	mhs     []multihash.Multihash
}

// getAdInfo fetches ad from provider via provider CLI
func getAdInfo(ctx context.Context, pid, addr, adCid string) (*adInfo, error) {
	out, err := runCmd(ctx, "provider", "ls", "ad", "--print-entries", fmt.Sprintf("--provider-addr-info=%s/p2p/%s", addr, pid), adCid)
	if err != nil {
		return nil, err
	}
	ai := &adInfo{
		mhs: make([]multihash.Multihash, 0, 10000),
	}
	s := bufio.NewScanner(bytes.NewBuffer(out))
	for s.Scan() {
		t := strings.TrimSpace(s.Text())
		if strings.HasPrefix(t, "PreviousID") {
			ai.prevCid = strings.TrimSpace(strings.Split(t, ":")[1])
		} else if strings.HasPrefix(t, "Qm") {
			mh, err := multihash.FromB58String(t)
			if err != nil {
				log.Printf("[%s] error parsing multihash=%v", t, err)
				continue
			}
			ai.mhs = append(ai.mhs, mh)
		}
	}
	return ai, nil
}

// compare compares two  multihash results
// returns whether multihashes results are equal with a list of mismatches
func compare(mhr1, mhr2 model.MultihashResult) (bool, []string) {
	if mhr1.Multihash.String() != mhr2.Multihash.String() {
		return false, []string{fmt.Sprintf("mismatching multihashes %s %s", mhr1.Multihash.String(), mhr2.Multihash.String())}
	}

	errors := []string{}
	mhr2Map := map[string]model.ProviderResult{}
	for _, pr := range mhr2.ProviderResults {
		mhr2Map[pr.Provider.ID.String()] = pr
	}

	for _, pr1 := range mhr1.ProviderResults {
		pr2, ok := mhr2Map[pr1.Provider.ID.String()]
		if !ok {
			continue
		}

		if !bytes.Equal(pr1.ContextID, pr2.ContextID) {
			errors = append(errors, fmt.Sprintf("mismatching context IDs %s %s", b58.Encode(pr1.ContextID), b58.Encode(pr2.ContextID)))
		}
		if !bytes.Equal(pr1.Metadata, pr2.Metadata) {
			errors = append(errors, fmt.Sprintf("mismatching metadata %s %s", b58.Encode(pr1.Metadata), b58.Encode(pr2.Metadata)))
		}
		if len(pr1.Provider.Addrs) != len(pr2.Provider.Addrs) {
			errors = append(errors, fmt.Sprintf("mismatching addrs %v %v", pr1.Provider.Addrs, pr2.Provider.Addrs))
		}

		addrs1 := make([]string, 0, len(pr1.Provider.Addrs))
		for _, a := range pr1.Provider.Addrs {
			addrs1 = append(addrs1, a.String())
		}
		addrs2 := make([]string, 0, len(pr2.Provider.Addrs))
		for _, a := range pr2.Provider.Addrs {
			addrs2 = append(addrs2, a.String())
		}
		sort.Strings(addrs1)
		sort.Strings(addrs2)

		for i := range addrs1 {
			if addrs1[i] != addrs2[i] {
				errors = append(errors, fmt.Sprintf("mismatching addrs %v %v", addrs1, addrs2))
			}
		}
	}
	return len(errors) == 0, errors
}

// runCmd executes a CLI command
func runCmd(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctxt, closer := context.WithTimeout(ctx, 10*time.Second)
	defer closer()
	cmd := exec.CommandContext(ctxt, name, args...)
	cmd.Env = os.Environ()
	return cmd.CombinedOutput()
}
