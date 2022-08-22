package ingest

import (
	"sync"

	"github.com/gammazero/deque"
)

type Queue struct {
	lk     sync.Mutex
	states map[providerID]uint32
	order  *deque.Deque[providerID]

	// used if someone is pulling an empty queue
	notifyChan chan struct{}
	doneChan   chan struct{}
}

func NewPriorityQueue() *Queue {
	return &Queue{
		states:     make(map[providerID]uint32),
		order:      deque.New[providerID](),
		notifyChan: make(chan struct{}),
		doneChan:   make(chan struct{}),
	}
}

// Push a provider into the set. returns the number of pushes this provider has had since last popped.
func (q *Queue) Push(p providerID) uint32 {
	q.lk.Lock()
	defer q.lk.Unlock()

	if _, ok := q.states[p]; !ok {
		q.order.PushBack(p)
		q.states[p] = 1
	} else {
		q.states[p]++
	}

	select {
	case q.notifyChan <- struct{}{}:
	default:
	}
	return q.states[p]
}

func (q *Queue) Pop() providerID {
	q.lk.Lock()

	for q.order.Len() == 0 {
		q.lk.Unlock()
		select {
		case <-q.doneChan:
			return ""
		case <-q.notifyChan:
		}
		q.lk.Lock()
	}

	p := q.order.PopFront()
	delete(q.states, p)
	q.lk.Unlock()
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
	return q.order.Len()
}

func (q *Queue) Close() {
	q.lk.Lock()
	defer q.lk.Unlock()
	close(q.doneChan)
}
