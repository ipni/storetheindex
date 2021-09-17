package config

const (
	defaultAllow = true
	defaultTrust = true
)

// Policy configures which providers are allowed and blocked, and which allowed
// providers require verification or are trusted
type Policy struct {
	// Allow is either false or true, and determines whether a provider is
	// allowed (true) or is blocked (false), by default.
	Allow bool
	// Except is a list of peer IDs that are exceptions to the allow action.
	// Providers that are allowed by policy or exception must still be verified
	// or trusted in order to register.
	Except []string

	// Trust is either false or true, and determines whether a provider can
	// skip (true) on-chain verification or not (false), by default
	Trust bool
	// TrustExcept is a list of peer IDs that are exceptions to the trust
	// action.  If Trust is false then all allowed providers must be
	// verified, except those listed here.  If Trust is true, then only the
	// providers listed here require verification.
	TrustExcept []string
}
