package config

// Policy configures which providers are allowed and blocked, and which allowed
// providers require verification or are trusted.
//
// Policy evaluation works like two gates that must be passed in order to be
// allowed to index content.  The first gate is the "allow" gate that
// determines whether a provider is allowd or not.  The second gate is the
// "trust" gate that determines whether a provider is trusted or must be
// verified on-chain to be authorized to index content.
type Policy struct {
	// Allow is either false or true, and determines whether a provider is
	// allowed (true) or is blocked (false), by default.
	Allow bool
	// Except is a list of peer IDs that are exceptions to the allow action.
	// Providers that are allowed by policy or exception must still be verified
	// or trusted in order to register.
	Except []string

	// Trust is either false or true, and determines whether an allowed
	// provider can skip (true) on-chain verification or not (false), by
	// default.
	Trust bool
	// TrustExcept is a list of peer IDs that are exceptions to the trust
	// action.  If Trust is false then all allowed providers must be
	// verified, except those listed here.  If Trust is true, then only the
	// providers listed here require verification.
	TrustExcept []string
}

// NewPolicy returns Policy with values set to their defaults.
func NewPolicy() Policy {
	return Policy{
		Allow: true,
		Trust: true,
	}
}
