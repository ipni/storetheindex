package federation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorClock(t *testing.T) {
	subject := newVectorClock()

	subject.tick("one")
	subject.tick("another")

	clock, found := subject.clock("one")
	require.True(t, found)
	require.Equal(t, uint64(1), clock)

	clock, found = subject.clock("another")
	require.True(t, found)
	require.Equal(t, uint64(1), clock)

	clock, found = subject.clock("and another")
	require.False(t, found)
	require.Equal(t, uint64(0), clock)

	subject.reconcile("another", 1413)
	clock, found = subject.clock("another")
	require.True(t, found)
	require.Equal(t, uint64(1413), clock)
}
