package ingest

import "sync"

type Queue struct {
	lk     sync.Mutex
	states map[providerID]adInfo
	order  []providerID

	// used if someone is pulling an empty queue
	waiters    uint
	notifyChan chan struct{}
	doneChan   chan struct{}
}

func NewPriorityQueue() *Queue {
	return &Queue{
		states:     make(map[providerID]adInfo),
		order:      []providerID{},
		waiters:    0,
		notifyChan: make(chan struct{}),
		doneChan:   make(chan struct{}),
	}
}

func (q *Queue) Push(p providerID, a adInfo) {
	q.lk.Lock()
	defer q.lk.Unlock()

	if _, ok := q.states[p]; !ok {
		q.order = append(q.order, p)
	}
	q.states[p] = a

	if q.waiters > 0 {
		q.notifyChan <- struct{}{}
	}
}

func (q *Queue) Pop() providerID {
	q.lk.Lock()
	defer q.lk.Unlock()

	for len(q.order) == 0 {
		q.waiters++
		q.lk.Unlock()
		select {
		case <-q.doneChan:
			return ""
		case <-q.notifyChan:
		}
		q.lk.Lock()
		q.waiters--
	}

	p := q.order[0]
	q.order = q.order[1:]
	delete(q.states, p)
	return p
}

// Returns a channel yielding the next provider to be pulled
// with 'at most once' semantics before the channel is closed.
func (q *Queue) PopChan() chan providerID {
	ch := make(chan providerID)

	go func() {
		defer close(ch)
		p := q.Pop()
		if p != "" {
			ch <- p
		}
	}()

	return ch
}

func (q *Queue) Has(p providerID) bool {
	q.lk.Lock()
	defer q.lk.Unlock()
	_, ok := q.states[p]
	return ok
}

func (q *Queue) Length() int {
	q.lk.Lock()
	defer q.lk.Unlock()
	return len(q.order)
}

func (q *Queue) Close() {
	q.lk.Lock()
	defer q.lk.Unlock()
	close(q.doneChan)
}
