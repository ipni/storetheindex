package peerutil

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Policy is a boolean value with a set of zero or more peer ID values.
// Evaluating a peer ID returns the boolean value, or its opposite if the peer
// ID is in the set of IDs.
//
// This serves a basis for simple policies where the boolean value applies to
// all peers except those in the set of peer IDs.
type Policy struct {
	value  bool
	except map[peer.ID]struct{}
}

// New creates a new Policy.
//
// The Policy evaluates to the given boolean value for all peers, except
// those listed in the except list.
func NewPolicy(value bool, except ...peer.ID) Policy {
	var exceptIDs map[peer.ID]struct{}
	if len(except) != 0 {
		exceptIDs = make(map[peer.ID]struct{}, len(except))
		for _, exceptID := range except {
			exceptIDs[exceptID] = struct{}{}
		}
	}

	return Policy{
		value:  value,
		except: exceptIDs,
	}
}

func NewPolicyStrings(value bool, except []string) (Policy, error) {
	var exceptIDs map[peer.ID]struct{}
	if len(except) != 0 {
		exceptIDs = make(map[peer.ID]struct{}, len(except))
		for _, exceptID := range except {
			peerID, err := peer.Decode(exceptID)
			if err != nil {
				return Policy{}, fmt.Errorf("error decoding peer id %q: %s", exceptID, err)
			}
			exceptIDs[peerID] = struct{}{}
		}
	}

	return Policy{
		value:  value,
		except: exceptIDs,
	}, nil
}

// Eval returns the boolean value for the specified peer.
func (p *Policy) Eval(peerID peer.ID) bool {
	_, ok := p.except[peerID]
	if p.value {
		return !ok
	}
	return ok
}

// Any returns true if any it is possible for a true value to be returned.
func (p *Policy) Any(value bool) bool {
	return value == p.value || len(p.except) != 0
}

// SetPeer ensures that the specified peer evaluates to the specified value,
// altering the except set if needed. Returns true if the except set was
// updated.
func (p *Policy) SetPeer(peerID peer.ID, value bool) bool {
	// If the specified value is not equal to the default value then add the
	// peerID as an exception.
	if value != p.value {
		if p.except == nil {
			p.except = make(map[peer.ID]struct{})
		}
		prevLen := len(p.except)
		p.except[peerID] = struct{}{}
		return len(p.except) != prevLen
	}

	// If the specified value is the same as the default value then remove the
	// peer from the exceptions.
	if len(p.except) != 0 {
		prevLen := len(p.except)
		delete(p.except, peerID)
		return len(p.except) != prevLen
	}
	return false
}

// Default returns the default value.
func (p *Policy) Default() bool {
	return p.value
}

// Except returns the except list as a slice of peer.ID.
func (p *Policy) Except() []peer.ID {
	if len(p.except) == 0 {
		return nil
	}
	except := make([]peer.ID, len(p.except))
	var i int
	for peerID := range p.except {
		except[i] = peerID
		i++
	}
	return except
}

// ExceptStrings returns the except list as a slice of peer.ID strings.
func (p *Policy) ExceptStrings() []string {
	if len(p.except) == 0 {
		return nil
	}
	exceptStrs := make([]string, len(p.except))
	var i int
	for peerID := range p.except {
		exceptStrs[i] = peerID.String()
		i++
	}
	return exceptStrs
}
