package policy

import (
	"testing"
	"time"

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
	providersCfg := config.Providers{
		Policy:         "block",
		Except:         []string{exceptIDStr},
		Trust:          []string{trustedIDStr},
		PollInterval:   config.Duration(time.Second),
		RediscoverWait: config.Duration(time.Second),
	}

	_, err := New(providersCfg)
	if err != nil {
		t.Fatal(err)
	}

	providersCfg.Policy = "allow"
	_, err = New(providersCfg)
	if err != nil {
		t.Fatal(err)
	}

	providersCfg.Policy = "foo"
	_, err = New(providersCfg)
	if err == nil {
		t.Fatal("expected error with bad Policy")
	}

	providersCfg.Policy = "block"
	providersCfg.Trust = append(providersCfg.Trust, "bad ID")
	_, err = New(providersCfg)
	if err == nil {
		t.Error("expected error with bad trust ID")
	}

	providersCfg.Trust = nil
	providersCfg.Except = append(providersCfg.Except, "bad ID")
	_, err = New(providersCfg)
	if err == nil {
		t.Error("expected error with bad except ID")
	}

	providersCfg.Except = nil
	_, err = New(providersCfg)
	if err == nil {
		t.Error("expected error with inaccessible policy")
	}

	providersCfg.Policy = "allow"
	_, err = New(providersCfg)
	if err != nil {
		t.Error(err)
	}
}

func TestPolicyAccess(t *testing.T) {
	providersCfg := config.Providers{
		Policy: "block",
		Except: []string{exceptIDStr},
		Trust:  []string{trustedIDStr},
	}

	p, err := New(providersCfg)
	if err != nil {
		t.Fatal(err)
	}

	if p.Trusted(exceptID) {
		t.Error("peer ID should not be trusted")
	}
	if !p.Trusted(trustedID) {
		for tid := range p.trust {
			t.Log("--->", tid)
		}
		t.Error("peer ID", trustedID, "should be trusted")
	}

	if p.Allowed(trustedID) {
		t.Error("peer ID should not be allowed by policy")
	}
	if !p.Allowed(exceptID) {
		t.Error("peer ID should be allowed")
	}

	providersCfg.Policy = "allow"
	p, err = New(providersCfg)
	if err != nil {
		t.Fatal(err)
	}

	if !p.Allowed(trustedID) {
		t.Error("peer ID should be allowed by policy")
	}
	if p.Allowed(exceptID) {
		t.Error("peer ID should not be allowed")
	}
}
