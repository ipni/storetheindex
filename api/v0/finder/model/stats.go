package model

import (
	"encoding/json"
)

// Stats is the client response to a stats request.
type Stats struct {
	EntriesEstimate int64
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
