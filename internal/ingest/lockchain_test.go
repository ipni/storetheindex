package ingest

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
)

func TestLockChain(t *testing.T) {
	wg := new(sync.WaitGroup)
	wg.Add(5)
	runList := make([]int, 0, 5)
	start := make(chan struct{})

	cids := make([]cid.Cid, 5)
	for i, s := range []string{
		"QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB",
		"QmQRuRVQTqzU76iQFQAhzWrgci7WGxLZPHd5Ky1WXUwiJZ",
		"QmWQadpxHe1UgAMdkZ5tm7znzqiixwo5u9XLKCtPGLtdDs",
		"Qmejoony52NYREWv3e9Ap6Uvg29GmJKJpxaDgAbzzYL9kX",
		"QmTrVTnL4pbnvrFRqyhDD96GyejSEh4kzS5fFCLowyA7GT",
	} {
		cids[i], _ = cid.Decode(s)
	}

	lc := newLockChain()

	w, u, err := lc.lockWait(cid.Undef, cids[0])
	if err != nil {
		t.Fatal(err)
	}
	go func(wait <-chan struct{}, unlock context.CancelFunc) {
		<-start
		<-wait
		defer unlock()

		// No mutext around runList, since wait() should serialize access.
		runList = append(runList, 1)
		wg.Done()
	}(w, u)

	w, u, err = lc.lockWait(cids[0], cids[1])
	if err != nil {
		t.Fatal(err)
	}
	go func(wait <-chan struct{}, unlock context.CancelFunc) {
		<-wait
		defer unlock()

		runList = append(runList, 2)
		wg.Done()
	}(w, u)

	w, u, err = lc.lockWait(cids[1], cids[2])
	if err != nil {
		t.Fatal(err)
	}
	go func(wait <-chan struct{}, unlock context.CancelFunc) {
		<-wait
		defer unlock()

		runList = append(runList, 3)
		wg.Done()
	}(w, u)

	w, u, err = lc.lockWait(cids[2], cids[3])
	if err != nil {
		t.Fatal(err)
	}
	go func(wait <-chan struct{}, unlock context.CancelFunc) {
		<-wait
		defer unlock()

		runList = append(runList, 4)
		wg.Done()
	}(w, u)

	ready := make(chan struct{})
	w, u, err = lc.lockWait(cids[3], cids[4])
	if err != nil {
		t.Fatal(err)
	}
	go func(wait <-chan struct{}, unlock context.CancelFunc) {
		close(ready)
		<-wait
		defer unlock()

		runList = append(runList, 5)
		wg.Done()
	}(w, u)

	<-ready
	close(start)
	wg.Wait()
	if !sort.IsSorted(sort.IntSlice(runList)) {
		t.Fatal("goroutines did not run in order", runList)
	}
	fmt.Println("done")
}
