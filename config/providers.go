package config

const (
	defaultPolicy       = "block"
	defaultPollInterval = "24h"
)

type Providers struct {
	// Policy is either "block" or "allow"
	Policy string
	// Except is a list of peer IDs that are excluded from the policy
	Except []string
	// PollInterval is the amount of time to wait without getting any updates
	// from a provider, before sending a request for the latest advertisement.
	// Values are a number ending in "s", "m", "h" for seconds. minutes, hours.
	PollInterval string
}
