package rate

import (
	"sync"
	"time"
)

type Rate struct {
	Count   uint64
	Elapsed time.Duration
	Samples int

	observed bool
}

// Map records the cumulative rate, since the last call to Get or GetAll,
// for each ID. Getting the rate
type Map struct {
	mutex sync.Mutex
	rates map[string]Rate
}

func NewMap() *Map {
	return &Map{
		rates: map[string]Rate{},
	}
}

// Update adds the new rate information to the existing information. If the
// existing rate information is markes as observed, then it is reset before
// applying the new information.
func (m *Map) Update(id string, count uint64, elapsed time.Duration) {
	if m == nil || count == 0 || elapsed == 0 {
		return
	}
	m.mutex.Lock()
	rate := m.rates[id]
	if rate.observed {
		rate = Rate{}
	}
	rate.Count += count
	rate.Elapsed += elapsed
	rate.Samples++
	m.rates[id] = rate
	m.mutex.Unlock()
}

// Get reads the accumulated rate information for the specified ID. The
// information is markes as observed and is reset at next write.
func (m *Map) Get(id string) (Rate, bool) {
	if m == nil {
		return Rate{}, false
	}
	m.mutex.Lock()
	r, ok := m.rates[id]
	if ok && !r.observed {
		r.observed = true
		m.rates[id] = r
	}
	m.mutex.Unlock()
	return r, ok
}

// GetAll reads and removes all accumulated rate information. The
// information is markes as observed and is reset at next write.
func (m *Map) GetAll() map[string]Rate {
	if m == nil {
		return nil
	}
	m.mutex.Lock()
	if len(m.rates) == 0 {
		m.mutex.Unlock()
		return nil
	}
	out := make(map[string]Rate, len(m.rates))
	for id, r := range m.rates {
		if !r.observed {
			r.observed = true
			m.rates[id] = r
		}
		out[id] = r
	}
	m.mutex.Unlock()
	return out
}
