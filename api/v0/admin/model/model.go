package model

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

type Assigned struct {
	Publisher peer.ID
	Continued peer.ID `json:",omitempty"`
}

type Handoff struct {
	FrozenID  peer.ID
	FrozenURL string
}

type Status struct {
	Frozen bool
	ID     peer.ID
}
