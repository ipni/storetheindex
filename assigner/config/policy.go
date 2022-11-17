package config

// Policy configures which peers are allowed and which are blocked. Announce
// messages are accepted from allowed peers and the publisher assigned to an
// indexer. Announce messagees from blocked peers are ignored.
type Policy struct {
	// Allow is either false or true, and determines whether a peer is allowed
	// (true) or is blocked (false), by default.
	Allow bool
	// Except is a list of peer IDs that are exceptions to the Allow policy.
	// If Allow is true, then all peers are allowed except those listed in
	// Except. If Allow is false, then no peers are allowed except those listed
	// in Except. in other words, Allow=true means that Except is a deny-list
	// and Allow=false means that Except is an allow-list.
	Except []string
}

// NewPolicy returns Policy with values set to their defaults.
func NewPolicy() Policy {
	return Policy{
		Allow: true,
	}
}
