package policy

import (
	"testing"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	exceptIDStr  = "12D3KooWK7CTS7cyWi51PeNE3cTjS2F2kDCZaQVU4A5xBmb9J1do"
	trustedIDStr = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
)

var (
	exceptID  peer.ID
	trustedID peer.ID
)

func init() {
	var err error
	exceptID, err = peer.Decode(exceptIDStr)
	if err != nil {
		panic(err)
	}
	trustedID, err = peer.Decode(trustedIDStr)
	if err != nil {
		panic(err)
	}
}

func TestNewPolicy(t *testing.T) {
	policyCfg := config.Policy{
		Allow:       false,
		Except:      []string{exceptIDStr},
		Trust:       false,
		TrustExcept: []string{trustedIDStr},
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
	policyCfg.TrustExcept = append(policyCfg.TrustExcept, "bad ID")
	_, err = New(policyCfg)
	if err == nil {
		t.Error("expected error with bad trust ID")
	}

	policyCfg.TrustExcept = nil
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
		Allow:       false,
		Except:      []string{exceptIDStr},
		Trust:       false,
		TrustExcept: []string{trustedIDStr},
	}

	p, err := New(policyCfg)
	if err != nil {
		t.Fatal(err)
	}

	if p.Allowed(trustedID) {
		t.Error("peer ID should not be allowed by policy")
	}
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed")
	}

	if p.Trusted(exceptID) {
		t.Error("peer ID should not be trusted")
	}
	if !p.Trusted(trustedID) {
		t.Error("peer ID", trustedID, "should be trusted")
	}

	policyCfg.Allow = true
	policyCfg.Trust = true
	p, err = New(policyCfg)
	if err != nil {
		t.Fatal(err)
	}

	if !p.Allowed(trustedID) {
		t.Error("peer ID should be allowed by policy")
	}
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}

	if !p.Trusted(exceptID) {
		t.Error("peer ID should not be trusted")
	}
	if p.Trusted(trustedID) {
		t.Error("peer ID", trustedID, "should be trusted")
	}
}
