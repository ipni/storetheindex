package announce

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdate(t *testing.T) {
	lru := newStringLRU(3)
	require.Zero(t, lru.len())

	hit := lru.update("hello")
	require.False(t, hit)
	require.Equal(t, 1, lru.len())

	hit = lru.update("hello")
	require.True(t, hit)
	require.Equal(t, 1, lru.len())

	lru.update("foo")
	lru.update("bar")
	require.Equal(t, 3, lru.len())
	lru.update("baz")
	require.Equal(t, 3, lru.len())

	hit = lru.update("hello")
	require.False(t, hit)

	hit = lru.update("bar")
	require.True(t, hit)

	require.True(t, lru.remove("bar"))
	require.False(t, lru.remove("bar"))
}
