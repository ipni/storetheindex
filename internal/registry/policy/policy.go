package policy

import (
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Policy struct {
	allow           bool
	except          map[peer.ID]struct{}
	publish         bool
	publishExcept   map[peer.ID]struct{}
	rateLimit       bool
	rateLimitExcept map[peer.ID]struct{}
	rwmutex         sync.RWMutex
}

func New(cfg config.Policy) (*Policy, error) {
	policy := &Policy{}

	err := policy.Config(cfg)
	if err != nil {
		return nil, err
	}
	return policy, nil
}

// Allowed returns true if the policy allows the peer to index content.
func (p *Policy) Allowed(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.allowed(peerID)
}

func (p *Policy) allowed(peerID peer.ID) bool {
	return evalPolicy(peerID, p.allow, p.except)
}

// PublishAllowed returns true if policy allows the publisher to publish
// advertisements for the identified provider.  This assumes that both are
// already allowed by policy.
func (p *Policy) PublishAllowed(publisherID, providerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.publishAllowed(publisherID, providerID)
}

func (p *Policy) publishAllowed(publisherID, providerID peer.ID) bool {
	if publisherID == providerID {
		return true
	}
	return evalPolicy(publisherID, p.publish, p.publishExcept)
}

// RateLimited determines if the peer is rate limited.
func (p *Policy) RateLimited(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.rateLimited(peerID)
}

func (p *Policy) rateLimited(peerID peer.ID) bool {
	return evalPolicy(peerID, p.rateLimit, p.rateLimitExcept)
}

func evalPolicy(peerID peer.ID, policy bool, exceptions map[peer.ID]struct{}) bool {
	_, ok := exceptions[peerID]
	if policy {
		return !ok
	}
	return ok
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
	p.publish = cfg.Publish
	p.rateLimit = cfg.RateLimit

	var err error
	p.except, err = getExceptPeerIDs(cfg.Except)
	if err != nil {
		return fmt.Errorf("cannot read except list: %s", err)
	}

	// Error if no peers are allowed
	if !p.allow && len(p.except) == 0 {
		return errors.New("policy does not allow any peers")
	}

	p.publishExcept, err = getExceptPeerIDs(cfg.PublishExcept)
	if err != nil {
		return fmt.Errorf("cannot read publish except list: %s", err)
	}

	p.rateLimitExcept, err = getExceptPeerIDs(cfg.RateLimitExcept)
	if err != nil {
		return fmt.Errorf("cannot read rate limit except list: %s", err)
	}

	return nil
}

// ToConfig converts a Policy into a config.Policy.
func (p *Policy) ToConfig() config.Policy {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	return config.Policy{
		Allow:           p.allow,
		Except:          getExceptStrings(p.except),
		Publish:         p.publish,
		PublishExcept:   getExceptStrings(p.publishExcept),
		RateLimit:       p.rateLimit,
		RateLimitExcept: getExceptStrings(p.rateLimitExcept),
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
