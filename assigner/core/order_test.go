package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrdering(t *testing.T) {
	pool := make([]indexerInfo, 10)
	for i := range pool {
		pool[i].assigned = int32(len(pool) - i)
	}

	assigner := &Assigner{
		indexerPool: pool,
	}

	candidates := make([]int, len(pool))
	for i := range candidates {
		candidates[i] = i
	}

	assigner.orderCandidates(candidates, []int{1, 3})
	require.Equal(t, []int{3, 1, 9, 8, 7, 6, 5, 4, 2, 0}, candidates)

	assigner.orderCandidates(candidates, candidates)
	require.Equal(t, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, candidates)

	assigner.orderCandidates(candidates, nil)
	require.Equal(t, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, candidates)

	assigner.indexerPool[5].assigned = 0
	assigner.orderCandidates(candidates, nil)
	require.Equal(t, []int{5, 9, 8, 7, 6, 4, 3, 2, 1, 0}, candidates)

	assigner.indexerPool[5].assigned = 0
	assigner.orderCandidates(candidates, []int{2, 0, 1})
	require.Equal(t, []int{2, 1, 0, 5, 9, 8, 7, 6, 4, 3}, candidates)
}
