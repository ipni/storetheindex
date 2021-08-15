package config

const defaultAction = "block"

// Policy configures which providers are allowed and blocked
type Policy struct {
	// Action is either "block" or "allow"
	Action string
	// Except is a list of peer IDs that are excluded from the policy action.
	// Providers that are allowed by policy or expetion must still be valid
	// providers.
	Except []string
	// Trust is a list of peer IDs that are allowed to register as providers
	// without being required to be vefified on-chain
	Trust []string
}
