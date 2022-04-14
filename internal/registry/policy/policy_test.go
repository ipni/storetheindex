package policy

import (
	"testing"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	exceptIDStr = "12D3KooWK7CTS7cyWi51PeNE3cTjS2F2kDCZaQVU4A5xBmb9J1do"
	otherIDStr  = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
)

var (
	exceptID peer.ID
	otherID  peer.ID
)

func init() {
	var err error
	exceptID, err = peer.Decode(exceptIDStr)
	if err != nil {
		panic(err)
	}
	otherID, err = peer.Decode(otherIDStr)
	if err != nil {
		panic(err)
	}
}

func TestNewPolicy(t *testing.T) {
	policyCfg := config.Policy{
		Allow:           false,
		Except:          []string{exceptIDStr},
		RateLimit:       false,
		RateLimitExcept: []string{exceptIDStr},
	}

	_, err := New(policyCfg)
	if err != nil {
		t.Fatal(err)
	}

	policyCfg.Allow = true
	_, err = New(policyCfg)
	if err != nil {
		t.Fatal(err)
	}

	policyCfg.Allow = false
	policyCfg.RateLimitExcept = append(policyCfg.RateLimitExcept, "bad ID")
	_, err = New(policyCfg)
	if err == nil {
		t.Error("expected error with bad RateLimitExcept ID")
	}

	policyCfg.RateLimitExcept = nil
	policyCfg.Except = append(policyCfg.Except, "bad ID")
	_, err = New(policyCfg)
	if err == nil {
		t.Error("expected error with bad except ID")
	}

	policyCfg.Except = nil
	_, err = New(policyCfg)
	if err == nil {
		t.Error("expected error with inaccessible policy")
	}

	policyCfg.Allow = true
	_, err = New(policyCfg)
	if err != nil {
		t.Error(err)
	}
}

func TestPolicyAccess(t *testing.T) {
	policyCfg := config.Policy{
		Allow:           false,
		Except:          []string{exceptIDStr},
		RateLimit:       false,
		RateLimitExcept: []string{exceptIDStr},
		Publish:         false,
		PublishExcept:   []string{exceptIDStr},
	}

	p, err := New(policyCfg)
	if err != nil {
		t.Fatal(err)
	}

	if p.Allowed(otherID) {
		t.Error("peer ID should not be allowed by policy")
	}
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed")
	}

	if p.PublishAllowed(otherID, exceptID) {
		t.Error("peer ID should not be allowed to publish")
	}
	if !p.PublishAllowed(otherID, otherID) {
		t.Error("peer ID should be allowed to publish to self")
	}
	if !p.PublishAllowed(exceptID, otherID) {
		t.Error("peer ID should be allowed to publish")
	}

	if p.RateLimited(otherID) {
		t.Error("peer ID should not be rate-limited")
	}
	if !p.RateLimited(exceptID) {
		t.Error("peer ID should be rate-limited")
	}

	p.Allow(otherID)
	if !p.Allowed(otherID) {
		t.Error("peer ID should be allowed by policy")
	}

	p.Block(exceptID)
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}

	policyCfg.Allow = true
	policyCfg.Publish = true
	policyCfg.RateLimit = true
	err = p.Config(policyCfg)
	if err != nil {
		t.Fatal(err)
	}

	if !p.Allowed(otherID) {
		t.Error("peer ID should be allowed by policy")
	}
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}

	if !p.PublishAllowed(otherID, exceptID) {
		t.Error("peer ID should be allowed to publish")
	}
	if p.PublishAllowed(exceptID, otherID) {
		t.Error("peer ID should not be allowed to publish")
	}
	if !p.PublishAllowed(exceptID, exceptID) {
		t.Error("peer ID be allowed to publish to self")
	}

	if !p.RateLimited(otherID) {
		t.Error("peer ID should be rate-limited")
	}
	if p.RateLimited(exceptID) {
		t.Error("peer ID should not be rate-limited")
	}

	p.Allow(exceptID)
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed by policy")
	}

	p.Block(otherID)
	if p.Allowed(otherID) {
		t.Error("peer ID should not be allowed")
	}
}
