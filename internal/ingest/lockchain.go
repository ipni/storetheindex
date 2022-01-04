package ingest

import (
	"context"
	"errors"
	"sync"

	"github.com/ipfs/go-cid"
)

// closedchan is a reusable closed channel.
var closedchan = make(chan struct{})

func init() {
	close(closedchan)
}

// lockChain orders the processing of items on a linked list so that they are
// run in the order that the items occur in the list, when the items are
// processed by concurrent goroutines.
type lockChain struct {
	mutex     sync.Mutex
	linkLocks map[cid.Cid]chan struct{}
}

func newLockChain() *lockChain {
	return &lockChain{
		linkLocks: make(map[cid.Cid]chan struct{}),
	}
}

// lockWait locks the current CID and returns a channel to wait for the previous CID to unlock.
// A function is also returned that unlocks the current CID.
func (lc *lockChain) lockWait(prevCid, curCid cid.Cid) (<-chan struct{}, context.CancelFunc, error) {
	if prevCid == curCid {
		panic("previous and current CIDs cannot be equal")
	}
	newChan := make(chan struct{})
	var prevChan chan struct{}
	var prevLocked bool
	var unlockFunc context.CancelFunc

	// Wait if the current CID is already busy. This can happen when two syncs
	// for the same content are happening concurrently.  Make one sync wait for
	// the other.
	lc.mutex.Lock()
	if curCid != cid.Undef {
		_, curLocked := lc.linkLocks[curCid]
		if curLocked {
			lc.mutex.Unlock()
			return nil, nil, errors.New("already locked")
		}

		// Add wait channel for the current CID.
		lc.linkLocks[curCid] = newChan

		unlockFunc = func() {
			close(newChan)
			lc.mutex.Lock()
			delete(lc.linkLocks, curCid)
			lc.mutex.Unlock()
		}
	} else {
		unlockFunc = func() {}
	}

	if prevCid != cid.Undef {
		// Lookup the previous wait channel.
		prevChan, prevLocked = lc.linkLocks[prevCid]
	}
	lc.mutex.Unlock()

	if !prevLocked {
		prevChan = closedchan
	}

	// Return wait channel and function to unlock the current CID.
	return prevChan, unlockFunc, nil
}
