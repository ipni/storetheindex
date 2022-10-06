package ingest

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type syncHandler struct {
	ing *Ingester
}

func (h *syncHandler) SetLatestSync(peer.ID, cid.Cid) {
	// NOOP
	// We don't care what go-legs tells us about it's latest sync, we are using
	// the latestSync mechanism to traverse to the last processed Ad.
	// This means that the only thing that sets our notion of latest sync is
	// marking an ad as processed.
}

func (h *syncHandler) GetLatestSync(p peer.ID) (cid.Cid, bool) {
	c, err := h.ing.GetLatestSync(p)
	if err != nil {
		return cid.Undef, false
	}
	return c, true
}
