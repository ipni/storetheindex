package ingest

import (
	"errors"
	"sync"

	"github.com/ipfs/go-cid"
)

type cidWaiter struct {
	mutex    sync.Mutex
	cidChans map[cid.Cid]chan struct{}
}

func newCidWaiter() *cidWaiter {
	return &cidWaiter{
		cidChans: make(map[cid.Cid]chan struct{}),
	}
}

func (w *cidWaiter) add(c cid.Cid) error {
	if c == cid.Undef {
		return nil
	}

	newChan := make(chan struct{})

	w.mutex.Lock()
	_, ok := w.cidChans[c]
	if ok {
		w.mutex.Unlock()
		return errors.New("already locked")
	}
	w.cidChans[c] = newChan
	w.mutex.Unlock()

	return nil
}

func (w *cidWaiter) wait(c cid.Cid) bool {
	if c == cid.Undef {
		return true
	}

	w.mutex.Lock()
	lock, ok := w.cidChans[c]
	w.mutex.Unlock()

	if !ok {
		return false
	}

	<-lock
	return true
}

func (w *cidWaiter) done(c cid.Cid) {
	if c == cid.Undef {
		return
	}

	w.mutex.Lock()
	lock, ok := w.cidChans[c]
	if ok {
		delete(w.cidChans, c)
	}
	w.mutex.Unlock()

	if !ok {
		return
	}

	close(lock)
}
