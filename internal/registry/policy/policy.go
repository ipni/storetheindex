package policy

import (
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Policy struct {
	allow       bool
	except      map[peer.ID]struct{}
	trust       bool
	trustExcept map[peer.ID]struct{}
	rwmutex     sync.RWMutex
}

func New(cfg config.Policy) (*Policy, error) {
	policy := &Policy{
		allow: cfg.Allow,
		trust: cfg.Trust,
	}

	err := policy.Config(cfg)
	if err != nil {
		return nil, err
	}
	return policy, nil
}

// Allowed returns true if the policy allows the peer to index content.  This
// check does not check whether the peer is trusted. An allowed peer must still
// be verified.
func (p *Policy) Allowed(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.allowed(peerID)
}

func (p *Policy) allowed(peerID peer.ID) bool {
	_, ok := p.except[peerID]
	if p.allow {
		return !ok
	}
	return ok
}

// Trusted returns true if the peer is explicitly trusted.  A trusted peer is
// allowed to register without requiring verification.
func (p *Policy) Trusted(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.trusted(peerID)
}

func (p *Policy) trusted(peerID peer.ID) bool {
	_, ok := p.trustExcept[peerID]
	if p.trust {
		return !ok
	}
	return ok
}

// Check returns whether the two bool values.  The fisrt is true if the peer is
// allowed.  The second is true if the peer is allowed and is trusted (does not
// require verification).
func (p *Policy) Check(peerID peer.ID) (bool, bool) {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	if !p.allowed(peerID) {
		return false, false
	}

	if !p.trusted(peerID) {
		return true, false
	}

	return true, true
}

// Allow alters the policy to allow the specified peer.  Returns true if the
// policy needed to be updated.
func (p *Policy) Allow(peerID peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	var updated bool
	if p.allow {
		if len(p.except) != 0 {
			prevLen := len(p.except)
			delete(p.except, peerID)
			updated = len(p.except) != prevLen
		}
	} else {
		if p.except == nil {
			p.except = make(map[peer.ID]struct{})
		}
		prevLen := len(p.except)
		p.except[peerID] = struct{}{}
		updated = len(p.except) != prevLen
	}

	return updated
}

// Block alters the policy to not allow the specified peer.  Returns true if
// the policy needed to be updated.
func (p *Policy) Block(peerID peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	var updated bool
	if p.allow {
		if p.except == nil {
			p.except = make(map[peer.ID]struct{})
		}
		prevLen := len(p.except)
		p.except[peerID] = struct{}{}
		updated = len(p.except) != prevLen
	} else if len(p.except) != 0 {
		prevLen := len(p.except)
		delete(p.except, peerID)
		updated = len(p.except) != prevLen
	}

	return updated
}

// Config applies the configuration.
func (p *Policy) Config(cfg config.Policy) error {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	p.allow = cfg.Allow
	p.trust = cfg.Trust

	var err error
	p.except, err = getExceptPeerIDs(cfg.Except)
	if err != nil {
		return fmt.Errorf("cannot read except list: %s", err)
	}

	// Error if no peers are allowed
	if !p.allow && len(p.except) == 0 {
		return errors.New("policy does not allow any peers")
	}

	p.trustExcept, err = getExceptPeerIDs(cfg.TrustExcept)
	if err != nil {
		return fmt.Errorf("cannot read trust except list: %s", err)
	}

	return nil
}

// ToConfig converts a Policy into a config.Policy.
func (p *Policy) ToConfig() config.Policy {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	return config.Policy{
		Allow:       p.allow,
		Except:      getExceptStrings(p.except),
		Trust:       p.trust,
		TrustExcept: getExceptStrings(p.trustExcept),
	}
}

func getExceptPeerIDs(excepts []string) (map[peer.ID]struct{}, error) {
	if len(excepts) == 0 {
		return nil, nil
	}

	exceptIDs := make(map[peer.ID]struct{}, len(excepts))
	for _, except := range excepts {
		excPeerID, err := peer.Decode(except)
		if err != nil {
			return nil, fmt.Errorf("error decoding peer id %q: %s", except, err)
		}
		exceptIDs[excPeerID] = struct{}{}
	}
	return exceptIDs, nil
}

func getExceptStrings(except map[peer.ID]struct{}) []string {
	exceptStrs := make([]string, len(except))
	var i int
	for peerID := range except {
		exceptStrs[i] = peerID.String()
		i++
	}
	return exceptStrs
}
