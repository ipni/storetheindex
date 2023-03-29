package model

import (
	"github.com/ipni/go-libipni/find/model"
)

// Deprecated: Use github.com/ipni/go-libipni/find/model.Stats instead
type Stats = model.Stats

// Deprecated: Use github.com/ipni/go-libipni/find/model.MarshalStats instead
func MarshalStats(s *Stats) ([]byte, error) {
	return model.MarshalStats(s)
}

// Deprecated: Use github.com/ipni/go-libipni/find/model.UnmarshalStats instead
func UnmarshalStats(b []byte) (*Stats, error) {
	return model.UnmarshalStats(b)
}
