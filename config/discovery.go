package config

import (
	"time"
)

// Discovery configures policy and polling settings that determines how
// information about available index data is discovered.
type Discovery struct {
	// FilterIPs, when true, removes any private, loopback, or unspecified IP
	// addresses from provider and publisher addresses.
	FilterIPs bool
	// IgnoreBadAdsTime is the period of time to ignore publishers that have
	// published unindexable advertisement chains. Default is 2 hours.
	IgnoreBadAdsTime Duration
	// Policy configures which publishers are allowed and blocked, rate-limited,
	// and allowed to publish on behalf of others providers.
	Policy Policy
	// PollInterval is the amount of time to wait without getting any updates
	// for a provider, before sending a request for the latest advertisement.
	// If there is no response after at least one poll attempt, then the
	// provider is considered inactive and is not returned in find responses.
	// Values are a number ending in "s", "m", "h" for seconds. minutes, hours.
	PollInterval Duration
	// PollRetryAfter is the amount of time from one poll attempt, without a
	// response, to the next poll attempt, and is also the time between checks
	// for providers to poll. This value must be smaller than PollStopAfter for
	// there to be more than one poll attempt for a provider.
	PollRetryAfter Duration
	// PollStopAfter is the amount of time, from the start of polling, to
	// continuing polling for the latest advertisement without getting a
	// response. After this time elapses with no updates from the provider, the
	// provider's data is removed from the indexer and must be re-fetched if
	// the provider returns.
	PollStopAfter Duration
	// DeactivateAfter is the duration after which an unseen provider will be
	// excluded from find queries as well as providers list. Note, an inactive
	// provider will not be deleted until PollStopAfter has elapses since last
	// it was seen. If unspecified, providers will be removed once PollStopAfter
	// has elapsed without entering inactive state grace period. If set to less
	// than PollStopAfter this parameter will have no effect.
	DeactivateAfter Duration
	// PollOverrides configures polling for specific providers.
	PollOverrides []Polling
	// RemoveOldAssignments, if true, removes persisted assignments of previous
	// versions. When false, previous versions of persisted assignments are
	// migrated. Only applies if UseAssigner is true.
	RemoveOldAssignments bool
	// UnassignedPublishers, if true, allows unassigned publishers to remain
	// unassigned until the assigner service assigns them, preferring this
	// indexer to assign the publishers to. If false (the default), assigns all
	// unassigned publishers known to this indexer.
	//
	// When configuring an existing indexer to use an assigner service, set
	// this to false if the known publishers should be immediately assigned to
	// this indexer to make them eligible for handoff if this indexer is
	// frozen. Set to true if the publishers known to this indexer may already
	// be assigned to another indexer, possibly making the assignment to this
	// indexer unnecessary.
	//
	// In general, it is safer to leave this set to false to guarantee that the
	// indexer is assigned the publishers it knows about. Otherwise, if the
	// indexer is frozen the unassigned publishers may not be handed off to
	// another indexer and an update from that publisher will result in
	// indexing all that publisher's data.
	UnassignedPublishers bool
	// UseAssigner configures the indexer to work with an assigner service.
	// This also requires that Policy.Allow is false, making Policy.Except into
	// a list of allowed peers. Peers listed in Policy.Except in the
	// configuration file are included in the set of assigned peers. This
	// allows an indexer to be pre-configured with assigned peers.
	UseAssigner bool
}

// Polling is a set of polling parameters that is applied to a specific
// provider. The values override the matching Poll values in the Discovery
// config.
type Polling struct {
	// ProviderID identifies the provider that this override applies to.
	ProviderID string
	// Interval overrides Discovery.PollInterval.
	Interval Duration
	// RetryAfter overrides Discovery.PollRetryAfter.
	RetryAfter Duration
	// StopAfter overrides Discovery.PollStopAfter.
	StopAfter Duration
	// DeactivateAfter overrides Discovery.DeactivateAfter.
	DeactivateAfter Duration
}

// NewDiscovery returns Discovery with values set to their defaults.
func NewDiscovery() Discovery {
	const defaultStopAfter = Duration(7 * 24 * time.Hour)
	return Discovery{
		IgnoreBadAdsTime: Duration(2 * time.Hour),
		Policy:           NewPolicy(),
		PollInterval:     Duration(24 * time.Hour),
		PollRetryAfter:   Duration(5 * time.Hour),
		PollStopAfter:    defaultStopAfter,
		DeactivateAfter:  defaultStopAfter,
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Discovery) populateUnset() {
	def := NewDiscovery()

	if c.IgnoreBadAdsTime == 0 {
		c.IgnoreBadAdsTime = def.IgnoreBadAdsTime
	}
	if c.PollInterval == 0 {
		c.PollInterval = def.PollInterval
	}
	if c.PollRetryAfter == 0 {
		c.PollRetryAfter = def.PollRetryAfter
	}
	if c.PollStopAfter == 0 {
		c.PollStopAfter = def.PollStopAfter
	}
	if c.DeactivateAfter == 0 {
		// Set deactivation to the same value as PollStopAfter.
		// This means no inactive grace period for providers by default.
		c.DeactivateAfter = def.PollStopAfter
	}
}
