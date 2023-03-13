package peerutil

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
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
	_, err := NewPolicyStrings(false, []string{exceptIDStr, "bad ID"})
	require.Error(t, err, "expected error with bad except ID")

	except := []string{exceptIDStr}

	p, err := NewPolicyStrings(false, except)
	require.NoError(t, err)
	require.True(t, p.Any(true), "true should be possible")

	p, err = NewPolicyStrings(true, except)
	require.NoError(t, err)
	require.True(t, p.Any(true), "true should be possible")

	p = NewPolicy(false)
	require.False(t, p.Any(true), "should not be true for any peers")

	require.False(t, p.SetPeer(exceptID, false), "should not have been updated to be false for peer")

	p = NewPolicy(true)
	require.True(t, p.Any(true), "should by true for any peers")

	require.True(t, p.SetPeer(exceptID, false), "should have been updated to be false for peer")
	require.False(t, p.Eval(exceptID), "should be false for peer ID")
}

func TestFalseDefault(t *testing.T) {
	p := NewPolicy(false, exceptID)

	require.False(t, p.Default(), "expected false default")

	require.False(t, p.Eval(otherID))
	require.True(t, p.Eval(exceptID))

	// Check that disabling otherID does not update.
	require.False(t, p.SetPeer(otherID, false), "should not have been updated")
	require.False(t, p.Eval(otherID))

	// Check that setting exceptID true does not update.
	require.False(t, p.SetPeer(exceptID, true), "should not have been updated")
	require.True(t, p.Eval(exceptID))

	// Check that setting otherID true does update.
	require.True(t, p.SetPeer(otherID, true), "should have been updated")
	require.True(t, p.Eval(otherID))

	// Check that setting exceptID false does update.
	require.True(t, p.SetPeer(exceptID, false), "should have been updated")
	require.False(t, p.Eval(exceptID))
}

func TestTrueDefault(t *testing.T) {
	p := NewPolicy(true, exceptID)

	require.True(t, p.Default(), "expected true default")

	require.True(t, p.Eval(otherID))
	require.False(t, p.Eval(exceptID))

	// Check that setting exceptID false does not update.
	require.False(t, p.SetPeer(exceptID, false), "should not have been updated")
	require.False(t, p.Eval(exceptID))

	// Check that setting otherID true does not update.
	require.False(t, p.SetPeer(otherID, true), "should not have been updated")
	require.True(t, p.Eval(otherID))

	// Check that setting exceptID true does updates.
	require.True(t, p.SetPeer(exceptID, true), "should have been updated")
	require.True(t, p.Eval(exceptID))

	// Check that setting otherID false does updates.
	require.True(t, p.SetPeer(otherID, false), "should have been updated")
	require.False(t, p.Eval(otherID))
}

func TestExceptStrings(t *testing.T) {
	p, err := NewPolicyStrings(false, nil)
	require.NoError(t, err)
	require.Zero(t, len(p.ExceptStrings()), "should not be any except strings")

	except := []string{exceptIDStr, otherIDStr}

	p, err = NewPolicyStrings(false, except)
	require.NoError(t, err)

	exStrs := p.ExceptStrings()
	require.Equal(t, 2, len(exStrs), "wrong number of except strings")

	for _, exStr := range exStrs {
		require.Contains(t, except, exStr, "except strings does not match original")
	}

	for exID := range p.except {
		p.SetPeer(exID, false)
	}

	require.Nil(t, p.ExceptStrings())
}
