package policy

import (
	"errors"
	"fmt"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Policy struct {
	allow       bool
	except      map[peer.ID]struct{}
	trust       bool
	trustExcept map[peer.ID]struct{}
}

func New(cfg config.Policy) (*Policy, error) {
	policy := &Policy{
		allow: cfg.Allow,
		trust: cfg.Trust,
	}

	var err error
	policy.except, err = getExceptPeerIDs(cfg.Except)
	if err != nil {
		return nil, fmt.Errorf("cannot read except list: %s", err)
	}

	// Error if no peers are allowed
	if !policy.allow && len(policy.except) == 0 {
		return nil, errors.New("policy does not allow any providers")
	}

	policy.trustExcept, err = getExceptPeerIDs(cfg.TrustExcept)
	if err != nil {
		return nil, fmt.Errorf("cannot read trust except list: %s", err)
	}

	return policy, nil
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

// Trusted returns true if the provider is explicitly trusted.  A trusted
// provider is allowed to register without requiring verification.
func (p *Policy) Trusted(providerID peer.ID) bool {
	_, ok := p.trustExcept[providerID]
	if p.trust {
		return !ok
	}
	return ok
}

// Allowed returns true if the policy allows the provider to index content.
// This check does not check whether the provider is trusted. An allowed
// provider must still be verified.
func (p *Policy) Allowed(providerID peer.ID) bool {
	_, ok := p.except[providerID]
	if p.allow {
		return !ok
	}
	return ok
}
