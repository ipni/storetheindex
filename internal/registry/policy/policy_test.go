package policy_test

import (
	"testing"

	"github.com/ipfs/go-test/random"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/registry/policy"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var (
	exceptID    peer.ID
	exceptIDStr string
	otherID     peer.ID
	otherIDStr  string
)

func init() {
	peers := random.Peers(2)
	exceptID = peers[0]
	otherID = peers[1]
	exceptIDStr = exceptID.String()
	otherIDStr = otherID.String()
}

func TestNewPolicy(t *testing.T) {
	policyCfg := config.Policy{
		Allow:         false,
		Except:        []string{exceptIDStr},
		Publish:       false,
		PublishExcept: []string{exceptIDStr},
	}

	_, err := policy.New(policyCfg)
	require.NoError(t, err)

	policyCfg.Allow = true
	_, err = policy.New(policyCfg)
	require.NoError(t, err)

	policyCfg.Allow = false
	policyCfg.PublishExcept = append(policyCfg.PublishExcept, "bad ID")
	_, err = policy.New(policyCfg)
	require.Error(t, err, "expected error with bad PublishExcept ID")
	policyCfg.PublishExcept = nil

	policyCfg.Except = append(policyCfg.Except, "bad ID")
	_, err = policy.New(policyCfg)
	require.Error(t, err, "expected error with bad except ID")
	policyCfg.Except = nil

	_, err = policy.New(policyCfg)
	require.NoError(t, err)

	policyCfg.Allow = true
	_, err = policy.New(policyCfg)
	require.NoError(t, err)
}

func TestPolicyAccess(t *testing.T) {
	policyCfg := config.Policy{
		Allow:         false,
		Except:        []string{exceptIDStr},
		Publish:       false,
		PublishExcept: []string{exceptIDStr},
	}

	p, err := policy.New(policyCfg)
	require.NoError(t, err)

	require.False(t, p.Allowed(otherID), "peer ID should not be allowed by policy")
	require.True(t, p.Allowed(exceptID), "peer ID should be allowed")

	require.False(t, p.PublishAllowed(otherID, exceptID), "peer ID should not be allowed to publish")
	require.True(t, p.PublishAllowed(otherID, otherID), "should be allowed to publish to self")
	require.False(t, p.PublishAllowed(exceptID, otherID), "should not be allowed to publish to blocked peer")

	p.Allow(otherID)
	require.True(t, p.Allowed(otherID), "peer ID should be allowed by policy")

	p.Block(exceptID)
	require.False(t, p.Allowed(exceptID), "peer ID should not be allowed")

	policyCfg.Allow = true
	policyCfg.Publish = true

	newPol, err := policy.New(policyCfg)
	require.NoError(t, err)
	p.Copy(newPol)

	require.True(t, p.Allowed(otherID), "peer ID should be allowed by policy")
	require.False(t, p.Allowed(exceptID), "peer ID should not be allowed")

	require.False(t, p.PublishAllowed(otherID, exceptID), "should not be allowed to publish to blocked peer")
	require.False(t, p.PublishAllowed(exceptID, otherID), "peer ID should not be allowed to publish")
	require.True(t, p.PublishAllowed(exceptID, exceptID), "peer ID be allowed to publish to self")

	p.Allow(exceptID)
	require.True(t, p.Allowed(exceptID), "peer ID should be allowed by policy")

	p.Block(otherID)
	require.False(t, p.Allowed(otherID), "peer ID should not be allowed")

	cfg := p.ToConfig()
	require.True(t, cfg.Allow)
	require.True(t, cfg.Publish)
	require.Len(t, cfg.Except, 1)
	require.Equal(t, otherIDStr, cfg.Except[0])
	require.Len(t, cfg.PublishExcept, 1)

	p, err = policy.New(config.Policy{})
	require.NoError(t, err)
	require.True(t, p.NoneAllowed(), "expected inaccessible policy")
}

func TestPublishersForProvider(t *testing.T) {
	peers := random.Peers(2)
	prov1 := peers[0]
	prov2 := peers[1]

	policyCfg := config.Policy{
		Allow:   true,
		Publish: false,
		PublishersForProvider: []config.PublishersPolicy{
			{
				Provider: prov1.String(),
				Allow:    false,
				Except:   []string{exceptIDStr},
			},
			{
				Provider: prov2.String(),
				Allow:    true,
				Except:   []string{exceptIDStr},
			},
		},
	}

	p, err := policy.New(policyCfg)
	require.NoError(t, err)

	require.True(t, p.PublishAllowed(prov1, prov1), "should be allowed to publish to self")
	require.False(t, p.PublishAllowed(exceptID, otherID))

	// Test publishing for prov1.
	require.False(t, p.PublishAllowed(otherID, prov1))
	require.True(t, p.PublishAllowed(exceptID, prov1))
	require.False(t, p.PublishAllowed(prov2, prov1))

	// Test publishing for prov1.
	require.True(t, p.PublishAllowed(otherID, prov2))
	require.False(t, p.PublishAllowed(exceptID, prov2))
	require.True(t, p.PublishAllowed(prov1, prov2))

	// Test with default policy set to default allow true.
	policyCfg.Publish = true
	p, err = policy.New(policyCfg)
	require.NoError(t, err)

	require.True(t, p.PublishAllowed(exceptID, otherID))
	require.False(t, p.PublishAllowed(exceptID, prov2))
	require.True(t, p.PublishAllowed(otherID, prov2))
	require.False(t, p.PublishAllowed(otherID, prov1))
}
