package federation

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

type vectorClock map[peer.ID]uint64

func newVectorClock() vectorClock {
	return make(map[peer.ID]uint64)
}

func (vc vectorClock) clock(id peer.ID) (uint64, bool) {
	clock, found := vc[id]
	return clock, found
}

func (vc vectorClock) tick(id peer.ID) uint64 {
	vc[id] = vc[id] + 1
	return vc[id]

}

func (vc vectorClock) untick(id peer.ID) uint64 {
	vc[id] = vc[id] - 1
	return vc[id]
}

func (vc vectorClock) reconcile(id peer.ID, c uint64) bool {
	if vc[id] < c {
		vc[id] = c
		return true
	}
	return false
}
