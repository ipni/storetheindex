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
	pol := &Policy{}

	err := pol.Config(cfg)
	if err != nil {
		return nil, err
	}
	return pol, nil
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

// Config applies the configuration.
func (p *Policy) Config(cfg config.Policy) error {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	except, err := peereval.StringsToPeerIDs(cfg.Except)
	if err != nil {
		return fmt.Errorf("bad allow policy: %s", err)
	}
	p.allow = peereval.New(cfg.Allow, except...)

	// Error if no peers are allowed.
	if !p.allow.Any(true) {
		return errors.New("policy does not allow any peers")
	}

	except, err = peereval.StringsToPeerIDs(cfg.PublishExcept)
	if err != nil {
		return fmt.Errorf("bad publish policy: %s", err)
	}
	p.publish = peereval.New(cfg.Publish, except...)

	except, err = peereval.StringsToPeerIDs(cfg.RateLimitExcept)
	if err != nil {
		return fmt.Errorf("bad rate limit policy: %s", err)
	}
	p.rateLimit = peereval.New(cfg.RateLimit, except...)

	return nil
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
