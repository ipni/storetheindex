package policy

import (
	"fmt"
	"sync"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/peerutil"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Policy struct {
	allow   peerutil.Policy
	publish peerutil.Policy
	rwmutex sync.RWMutex
}

func New(cfg config.Policy) (*Policy, error) {
	allow, err := peerutil.NewPolicyStrings(cfg.Allow, cfg.Except)
	if err != nil {
		return nil, fmt.Errorf("bad allow policy: %s", err)
	}

	publish, err := peerutil.NewPolicyStrings(cfg.Publish, cfg.PublishExcept)
	if err != nil {
		return nil, fmt.Errorf("bad publish policy: %s", err)
	}

	return &Policy{
		allow:   allow,
		publish: publish,
	}, nil
}

// Allowed returns true if the policy allows the peer to index content.
func (p *Policy) Allowed(peerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()
	return p.allow.Eval(peerID)
}

// PublishAllowed returns true if policy allows the publisher to publish
// advertisements for the identified provider, and the provider is allowed.
func (p *Policy) PublishAllowed(publisherID, providerID peer.ID) bool {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	// Publisher is always allowed to publish to self.
	if publisherID == providerID {
		return true
	}
	// Publisher may not publish advertisements for a provider that is not
	// allowed to register.
	if !p.allow.Eval(providerID) {
		return false
	}
	return p.publish.Eval(publisherID)
}

// Allow alters the policy to allow the specified peer. Returns true if the
// policy needed to be updated.
func (p *Policy) Allow(peerIDs ...peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	var updated bool
	for _, peerID := range peerIDs {
		if p.allow.SetPeer(peerID, true) {
			updated = true
		}
	}
	return updated
}

// Block alters the policy to not allow the specified peer.  Returns true if
// the policy needed to be updated.
func (p *Policy) Block(peerIDs ...peer.ID) bool {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	var updated bool
	for _, peerID := range peerIDs {
		if p.allow.SetPeer(peerID, false) {
			updated = true
		}
	}
	return updated
}

// Copy copies another policy.
func (p *Policy) Copy(other *Policy) {
	p.rwmutex.Lock()
	defer p.rwmutex.Unlock()

	other.rwmutex.RLock()
	p.allow = other.allow
	p.publish = other.publish
	other.rwmutex.RUnlock()
}

// ToConfig converts a Policy into a config.Policy.
func (p *Policy) ToConfig() config.Policy {
	p.rwmutex.RLock()
	defer p.rwmutex.RUnlock()

	return config.Policy{
		Allow:         p.allow.Default(),
		Except:        p.allow.ExceptStrings(),
		Publish:       p.publish.Default(),
		PublishExcept: p.publish.ExceptStrings(),
	}
}

// Return true if no peers are allowed.
func (p *Policy) NoneAllowed() bool {
	return !p.allow.Any(true)
}
