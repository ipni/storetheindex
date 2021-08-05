package config

const (
	defaultPolicy       = "block"
	defaultPollInterval = "24h"
)

type Providers struct {
	// Policy is either "block" or "allow"
	Policy string
	// Except is a list of peer IDs that are excluded from the policy.
	// Providers that are allowed by policy or expetion must still be valid
	// providers.
	Except []string
	// Trust is a list of peer IDs that are allowed with being required to be
	// valid providers.
	Trust []string
	// PollInterval is the amount of time to wait without getting any updates
	// from a provider, before sending a request for the latest advertisement.
	// Values are a number ending in "s", "m", "h" for seconds. minutes, hours.
	PollInterval string
}
