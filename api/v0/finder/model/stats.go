package model

import (
	"encoding/json"
)

// Stats is the client response to a stats request.
type Stats struct {
	// TODO: once we have grown confident of EntriesCount accuracy remove EntriesEstimate.

	// EntriesEstimate estimates the number of entries by simply dividing the size of valuestore by
	// a constant (40). Warning: this estimation does not take into account the backing store
	// optimisations such as compression an prefixed-based storage which could significantly reduce
	// size on disk.
	EntriesEstimate int64
	// EntriesCount uses the backing store API whenever possible to count the number of unique
	// entries in the value store. Its value is not an exact count. It is considered to be far more
	// accurate than estimates based on size of valuestore, e.g. EntriesEstimate.
	EntriesCount int64
}

// MarshalStats serializes the stats response. Currently uses JSON, but could
// use anything else.
func MarshalStats(s *Stats) ([]byte, error) {
	return json.Marshal(s)
}

// UnmarshalStats de-serializes the stats response.
func UnmarshalStats(b []byte) (*Stats, error) {
	s := &Stats{}
	err := json.Unmarshal(b, s)
	return s, err
}
