package ingest

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
)

// lockChain orders the processing of items on a linked list so that they are
// run in the order that the items occur in the list, when the items processed
// by concurrent goroutines.
type lockChain struct {
	mutex     sync.Mutex
	linkLocks map[cid.Cid]chan struct{}
}

func newLockChain() *lockChain {
	return &lockChain{
		linkLocks: make(map[cid.Cid]chan struct{}),
	}
}

// lockWait locks the current CID then waits for the previous CID to unlock.
// Then a function is returned that unlocks the current CID.
func (lc *lockChain) lockWait(curCid, prevCid cid.Cid) context.CancelFunc {
	if prevCid == curCid {
		panic("previous and current CIDs cannot be equal")
	}
	newChan := make(chan struct{})
	var prevChan chan struct{}
	var prevLocked bool

	// Wait if the current CID is already busy. This can happen when two syncs for the
	// same content are happening concurrently.  Make one sync wait for the other.
	for {
		lc.mutex.Lock()
		curChan, curLocked := lc.linkLocks[curCid]
		if !curLocked {
			break
		}
		lc.mutex.Unlock()
		<-curChan
	}

	// Add the new current chan and look up the previous channel.
	lc.linkLocks[curCid] = newChan
	if prevCid != cid.Undef {
		prevChan, prevLocked = lc.linkLocks[prevCid]
	}
	lc.mutex.Unlock()

	// Wait for previous CID channel, if there is one.
	if prevLocked {
		<-prevChan
	}

	// Return a function to unlock the current CID.
	return func() {
		close(newChan)
		lc.mutex.Lock()
		delete(lc.linkLocks, curCid)
		lc.mutex.Unlock()
	}
}
