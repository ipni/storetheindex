package ingest

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
)

func TestLockChain(t *testing.T) {
	mutex := new(sync.Mutex)
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

	go func() {
		unlock := lc.lockWait(cid.Undef, cids[0])
		defer unlock()

		mutex.Lock()
		defer mutex.Unlock()

		<-start

		runList = append(runList, 1)
		wg.Done()
	}()

	time.Sleep(250 * time.Millisecond)
	go func() {
		unlock := lc.lockWait(cids[1], cids[0])
		defer unlock()

		mutex.Lock()
		defer mutex.Unlock()
		runList = append(runList, 2)
		wg.Done()
	}()

	time.Sleep(250 * time.Millisecond)
	go func() {
		unlock := lc.lockWait(cids[2], cids[1])
		defer unlock()

		runList = append(runList, 3)
		wg.Done()
	}()

	time.Sleep(250 * time.Millisecond)
	go func() {
		unlock := lc.lockWait(cids[3], cids[2])
		defer unlock()

		mutex.Lock()
		defer mutex.Unlock()
		runList = append(runList, 4)
		wg.Done()
	}()

	time.Sleep(250 * time.Millisecond)
	ready := make(chan struct{})
	go func() {
		close(ready)
		unlock := lc.lockWait(cids[4], cids[3])
		defer unlock()

		mutex.Lock()
		defer mutex.Unlock()
		runList = append(runList, 5)
		wg.Done()
	}()

	<-ready
	close(start)
	wg.Wait()
	if !sort.IsSorted(sort.IntSlice(runList)) {
		t.Fatal("goroutines did not run in order")
	}
	fmt.Println("done")
}
