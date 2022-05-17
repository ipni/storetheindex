package config

type RateLimit struct {
	// Apply is either false or true, and determines whether a peer is subject
	// to rate limiting (true) or not (false), by default.
	Apply bool
	// Except is a list of peer IDs that are exceptions to the Apply rule. If
	// Apply is false then peers are not rate-limited unless they appear in the
	// Except list. If Apply is true, then only the peers listed in Except are
	// not rate-limited.
	Except []string
	// BlocksPerSecond is the number of blocks allowed to be transferred per
	// second. An advertisement and a block of multihashes are both represented
	// as a block, so this limit applies to both. Setting a value of 0 disables
	// rate limiting, meaning that the rate is infinite.
	BlocksPerSecond int
	// BurstSize is the maximum number of blocks that can be received at once.
	// After this, BlocksPerSecond additional blocks maybe received each
	// second, and any more results in rate limiting. With HTTP ingestion, rate
	// limiting waits until more blocks are allowed to be received. With
	// graphsync, rate limiting terminates the session and resumes it when
	// sufficient time has passed to be able to receive BurstSize blocks. A
	// value of 0 results in 10 times BlocksPerSecond.
	BurstSize int
}

// NewRateLimit returns RateLimit with values set to their defaults.
func NewRateLimit() RateLimit {
	return RateLimit{
		BlocksPerSecond: 100,
		BurstSize:       500,
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *RateLimit) populateUnset() {
	if c.BurstSize == 0 {
		c.BurstSize = 5 * c.BlocksPerSecond
	}
}
