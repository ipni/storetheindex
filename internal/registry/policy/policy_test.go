package policy

import (
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/libp2p/go-libp2p/core/peer"
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
		Allow:         false,
		Except:        []string{exceptIDStr},
		Publish:       false,
		PublishExcept: []string{exceptIDStr},
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
	policyCfg.PublishExcept = append(policyCfg.PublishExcept, "bad ID")
	_, err = New(policyCfg)
	if err == nil {
		t.Error("expected error with bad PublishExcept ID")
	}
	policyCfg.PublishExcept = nil

	policyCfg.Except = append(policyCfg.Except, "bad ID")
	_, err = New(policyCfg)
	if err == nil {
		t.Error("expected error with bad except ID")
	}
	policyCfg.Except = nil

	_, err = New(policyCfg)
	if err != nil {
		t.Error(err)
	}

	policyCfg.Allow = true
	_, err = New(policyCfg)
	if err != nil {
		t.Error(err)
	}
}

func TestPolicyAccess(t *testing.T) {
	policyCfg := config.Policy{
		Allow:         false,
		Except:        []string{exceptIDStr},
		Publish:       false,
		PublishExcept: []string{exceptIDStr},
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
		t.Error("should be allowed to publish to self")
	}
	if p.PublishAllowed(exceptID, otherID) {
		t.Error("should not be allowed to publish to blocked peer")
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

	newPol, err := New(policyCfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Copy(newPol)

	if !p.Allowed(otherID) {
		t.Error("peer ID should be allowed by policy")
	}
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}

	if p.PublishAllowed(otherID, exceptID) {
		t.Error("should not be allowed to publish to blocked peer")
	}
	if p.PublishAllowed(exceptID, otherID) {
		t.Error("peer ID should not be allowed to publish")
	}
	if !p.PublishAllowed(exceptID, exceptID) {
		t.Error("peer ID be allowed to publish to self")
	}

	p.Allow(exceptID)
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed by policy")
	}

	p.Block(otherID)
	if p.Allowed(otherID) {
		t.Error("peer ID should not be allowed")
	}

	cfg := p.ToConfig()
	if cfg.Allow != true {
		t.Error("wrong config.Allow")
	}
	if cfg.Publish != true {
		t.Error("wrong config.Publish")
	}
	if len(cfg.Except) != 1 {
		t.Fatal("expected 1 item in cfg.Except")
	}
	if cfg.Except[0] != otherIDStr {
		t.Error("wrong ID in cfg.Except")
	}
	if len(cfg.PublishExcept) != 1 {
		t.Fatal("expected 1 item in cfg.PublishExcept")
	}

	p, err = New(config.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if !p.NoneAllowed() {
		t.Error("expected inaccessible policy")
	}
}
