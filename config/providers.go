package config

import "time"

const (
	defaultPolicy           = "block"
	defaultPollInterval     = Duration(24 * time.Hour)
	defaultDiscoveryTimeout = Duration(2 * time.Minute)
	defaultRediscoverWait   = Duration(5 * time.Minute)
)

type Providers struct {
	// Policy is either "block" or "allow"
	Policy string
	// Except is a list of peer IDs that are excluded from the policy.
	// Providers that are allowed by policy or expetion must still be valid
	// providers.
	Except []string
	// Trust is a list of peer IDs that are allowed to register as providers
	// without being required to be vefified on-chain
	Trust []string
	// PollInterval is the amount of time to wait without getting any updates
	// from a provider, before sending a request for the latest advertisement.
	// Values are a number ending in "s", "m", "h" for seconds. minutes, hours.
	PollInterval Duration

	// DiscoveryTimeout is the maximum amount of time that the indexer will
	// spend trying to verify a new provider.
	DiscoveryTimeout Duration
	// RediscoverWait is the amount of time that must pass before a provider
	// can be discovered following a previous discovery attempt
	RediscoverWait Duration
}
