package ingest

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
)

func TestCidWaiter(t *testing.T) {
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

	w := newCidWaiter()

	err := w.add(cids[4])
	if err != nil {
		t.Fatal(err)
	}
	err = w.add(cids[3])
	if err != nil {
		t.Fatal(err)
	}
	err = w.add(cids[2])
	if err != nil {
		t.Fatal(err)
	}
	err = w.add(cids[1])
	if err != nil {
		t.Fatal(err)
	}
	err = w.add(cids[0])
	if err != nil {
		t.Fatal(err)
	}

	go func(prevCid, c cid.Cid) {
		<-start
		w.wait(prevCid)
		defer wc.done(c)

		// No mutext around runList, since wait() should serialize access.
		runList = append(runList, 1)
		wg.Done()
	}(cid.Undef, cids[0])

	go func(prevCid, c cid.Cid) {
		w.wait(prevCid)
		defer wc.done(c)

		runList = append(runList, 2)
		wg.Done()
	}(cids[0], cids[1])

	go func(prevCid, c cid.Cid) {
		w.wait(prevCid)
		defer wc.done(c)

		runList = append(runList, 3)
		wg.Done()
	}(cids[1], cids[2])

	go func(prevCid, c cid.Cid) {
		w.wait(prevCid)
		defer wc.done(c)

		runList = append(runList, 4)
		wg.Done()
	}(cids[2], cids[3])

	ready := make(chan struct{})
	go func(prevCid, c cid.Cid) {
		close(ready)
		w.wait(prevCid)
		defer wc.done(c)

		runList = append(runList, 5)
		wg.Done()
	}(cids[3], cids[4])

	<-ready
	close(start)
	wg.Wait()
	if !sort.IsSorted(sort.IntSlice(runList)) {
		t.Fatal("goroutines did not run in order", runList)
	}
	fmt.Println("done")
}
