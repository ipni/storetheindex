package peereval

import (
	"testing"

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

func TestNew(t *testing.T) {
	_, err := NewStrings(false, []string{exceptIDStr, "bad ID"})
	if err == nil {
		t.Error("expected error with bad except ID")
	}

	except := []string{exceptIDStr}

	p, err := NewStrings(false, except)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Any(true) {
		t.Error("true should be possible")
	}

	p, err = NewStrings(true, except)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Any(true) {
		t.Error("true should be possible")
	}

	p = New(false)
	if p.Any(true) {
		t.Error("should not be true for any peers")
	}

	if p.SetPeer(exceptID, false) {
		t.Fatal("should not have been updated to be false for peer")
	}

	p = New(true)
	if !p.Any(true) {
		t.Error("should by true for any peers")
	}

	if !p.SetPeer(exceptID, false) {
		t.Fatal("should have been updated to be false for peer")
	}
	if p.Eval(exceptID) {
		t.Fatal("should be false for peer ID")
	}
}

func TestFalseDefault(t *testing.T) {
	p := New(false, exceptID)

	if p.Default() {
		t.Fatal("expected false default")
	}

	if p.Eval(otherID) {
		t.Error("should evaluate false")
	}
	if !p.Eval(exceptID) {
		t.Error("should evaluate true")
	}

	// Check that disabling otherID does not update.
	if p.SetPeer(otherID, false) {
		t.Fatal("should not have been updated")
	}
	if p.Eval(otherID) {
		t.Error("should not evaluate true")
	}

	// Check that setting exceptID true does not update.
	if p.SetPeer(exceptID, true) {
		t.Fatal("should not have been updated")
	}
	if !p.Eval(exceptID) {
		t.Error("should evaluate true")
	}

	// Check that setting otherID true does update.
	if !p.SetPeer(otherID, true) {
		t.Fatal("should have been updated")
	}
	if !p.Eval(otherID) {
		t.Error("should evaluate true")
	}

	// Check that setting exceptID false does update.
	if !p.SetPeer(exceptID, false) {
		t.Fatal("should have been updated")
	}
	if p.Eval(exceptID) {
		t.Error("peer ID should evaluate false")
	}
}

func TestTrueDefault(t *testing.T) {
	p := New(true, exceptID)

	if !p.Default() {
		t.Fatal("expected true default")
	}

	if !p.Eval(otherID) {
		t.Error("should evaluate true")
	}
	if p.Eval(exceptID) {
		t.Error("should evaluate false")
	}

	// Check that setting exceptID false does not update.
	if p.SetPeer(exceptID, false) {
		t.Fatal("should not have been update")
	}
	if p.Eval(exceptID) {
		t.Error("should evaluate false")
	}

	// Check that setting otherID true does not update.
	if p.SetPeer(otherID, true) {
		t.Fatal("should have been update")
	}
	if !p.Eval(otherID) {
		t.Error("should evaluate true")
	}

	// Check that setting exceptID true does updates.
	if !p.SetPeer(exceptID, true) {
		t.Fatal("should have been update")
	}
	if !p.Eval(exceptID) {
		t.Error("should evaluate true")
	}

	// Check that setting otherID false does updates.
	if !p.SetPeer(otherID, false) {
		t.Fatal("should have been updated")
	}
	if p.Eval(otherID) {
		t.Error("should evaluate false")
	}
}

func TestExceptStrings(t *testing.T) {
	p, err := NewStrings(false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.ExceptStrings()) != 0 {
		t.Fatal("should not be any except strings")
	}

	except := []string{exceptIDStr, otherIDStr}

	p, err = NewStrings(false, except)
	if err != nil {
		t.Fatal(err)
	}

	exStrs := p.ExceptStrings()
	if len(exStrs) != 2 {
		t.Fatal("wrong number of except strings")
	}

	for _, exStr := range exStrs {
		if exStr != except[0] && exStr != except[1] {
			t.Fatal("except strings does not match original")
		}
	}

	for exID := range p.except {
		p.SetPeer(exID, false)
	}

	if p.ExceptStrings() != nil {
		t.Fatal("expected nil except strings")
	}
}
