package registry

import (
	"errors"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

const defaultMaxAge = 48 * time.Hour

type sequences struct {
	maxAge time.Duration
	mutex  sync.Mutex
	seqs   map[peer.ID]uint64
}

func newSequences(maxAge time.Duration) *sequences {
	if maxAge == 0 {
		maxAge = defaultMaxAge
	}
	return &sequences{
		maxAge: maxAge,
		seqs:   make(map[peer.ID]uint64),
	}
}

func (s *sequences) check(id peer.ID, sequence uint64) error {
	oldestAllowed := uint64(time.Now().Add(-s.maxAge).UnixNano())
	if sequence < oldestAllowed {
		return errors.New("sequence too small")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	prevSeq, ok := s.seqs[id]
	if ok && sequence <= prevSeq {
		return errors.New("sequence less than or equal to last seen")
	}
	s.seqs[id] = sequence
	return nil
}

func (s *sequences) retire() {
	oldestAllowed := uint64(time.Now().Add(-s.maxAge).UnixNano())
	active := make(map[peer.ID]uint64)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for id, seq := range s.seqs {
		if seq > oldestAllowed {
			active[id] = seq
		}
	}
	s.seqs = active
}
