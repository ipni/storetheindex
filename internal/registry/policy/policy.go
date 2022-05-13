package policy

import (
	"fmt"
	"sync"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/peerutil"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Policy struct {
	allow     peerutil.PeerEval
	publish   peerutil.PeerEval
	rateLimit peerutil.PeerEval
	rwmutex   sync.RWMutex
}

func New(cfg config.Policy) (*Policy, error) {
	allow, err := peerutil.NewStrings(cfg.Allow, cfg.Except)
	if err != nil {
		return nil, fmt.Errorf("bad allow policy: %s", err)
	}

	publish, err := peerutil.NewStrings(cfg.Publish, cfg.PublishExcept)
	if err != nil {
		return nil, fmt.Errorf("bad publish policy: %s", err)
	}

	rateLimit, err := peerutil.NewStrings(cfg.RateLimit, cfg.RateLimitExcept)
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

// Return true if no peers are allowed.
func (p *Policy) NoneAllowed() bool {
	return !p.allow.Any(true)
}
