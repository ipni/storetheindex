package policy

import (
	"errors"
	"fmt"
	"sync"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/peereval"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Policy struct {
	allow     peereval.PeerEval
	publish   peereval.PeerEval
	rateLimit peereval.PeerEval
	rwmutex   sync.RWMutex
}

func New(cfg config.Policy) (*Policy, error) {
	allow, err := peereval.NewStrings(cfg.Allow, cfg.Except)
	if err != nil {
		return nil, fmt.Errorf("bad allow policy: %s", err)
	}

	// Error if no peers are allowed.
	if !allow.Any(true) {
		return nil, errors.New("policy does not allow any peers")
	}

	publish, err := peereval.NewStrings(cfg.Publish, cfg.PublishExcept)
	if err != nil {
		return nil, fmt.Errorf("bad publish policy: %s", err)
	}

	rateLimit, err := peereval.NewStrings(cfg.RateLimit, cfg.RateLimitExcept)
	if err != nil {
		return nil, fmt.Errorf("bad rate limit policy: %s", err)
	}

	return &Policy{
		allow:     allow,
		publish:   publish,
		rateLimit: rateLimit,
	}, nil
}

// Allowed returns true if the policy allows the peer to index content.
func (p *Policy) Allowed(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.allow.Eval(peerID)
}

// PublishAllowed returns true if policy allows the publisher to publish
// advertisements for the identified provider.  This assumes that both are
// already allowed by policy.
func (p *Policy) PublishAllowed(publisherID, providerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	if publisherID == providerID {
		return true
	}
	return p.publish.Eval(publisherID)
}

// RateLimited determines if the peer is rate limited.
func (p *Policy) RateLimited(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.rateLimit.Eval(peerID)
}

// Allow alters the policy to allow the specified peer.  Returns true if the
// policy needed to be updated.
func (p *Policy) Allow(peerID peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()
	return p.allow.SetPeer(peerID, true)
}

// Block alters the policy to not allow the specified peer.  Returns true if
// the policy needed to be updated.
func (p *Policy) Block(peerID peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()
	return p.allow.SetPeer(peerID, false)
}

// Copy copies another policy.
func (p *Policy) Copy(other *Policy) {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	other.rwmutex.RLock()
	p.allow = other.allow
	p.publish = other.publish
	p.rateLimit = other.rateLimit
	other.rwmutex.RUnlock()
}

// ToConfig converts a Policy into a config.Policy.
func (p *Policy) ToConfig() config.Policy {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	return config.Policy{
		Allow:           p.allow.Default(),
		Except:          p.allow.ExceptStrings(),
		Publish:         p.publish.Default(),
		PublishExcept:   p.publish.ExceptStrings(),
		RateLimit:       p.rateLimit.Default(),
		RateLimitExcept: p.rateLimit.ExceptStrings(),
	}
}
