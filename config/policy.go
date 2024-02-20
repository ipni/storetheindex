package config

// Policy configures which peers are allowed to be providers, publish
// advertisements and publish on behalf of other providers. The Allow policy
// determines which peers the indexer will request advertisements from and
// index content for. The Publish policy determines if a publisher may supply
// an advertisement that has a provider that is different from the publisher.
//
// Publishers and providers are not the same. Publishers are peers that supply
// data to the indexer. Providers are the peers that appear in advertisements
// and are where retrieval clients get content from.
type Policy struct {
	// Allow is either false or true, and determines whether a peer is allowed
	// (true) or is blocked (false), by default. If a peer if blocked, then it
	// cannot publish advertisements to this indexer or be listed as a provider
	// by this indexer.
	Allow bool
	// Except is a list of peer IDs that are exceptions to the Allow policy.
	// If Allow is true, then all peers are allowed except those listed in
	// Except. If Allow is false, then no peers are allowed except those listed
	// in Except. in other words, Allow=true means that Except is a deny-list
	// and Allow=false means that Except is an allow-list.
	Except []string
	// Publish is the default Allow policy when a provider has no policy in
	// PublisherForProviders. It determines if any peers are allowed to publish
	// advertisements for a provider with a differen peer ID.
	Publish bool
	// PublisherExcept is a list of peer IDs that are exceptions to the default
	// Publish policy.
	PublishExcept []string
	// PublishersForProvider is a list of policies specifying which publishers
	// are allowed to publish advertisements on behalf of a specified provider.
	PublishersForProvider []PublishersPolicy
}

type PublishersPolicy struct {
	// Provider is the provider peer ID this policy pertains to.
	Provider string
	// Allow determines whether a peer is allowed or blocked from publishing
	// advertisements on behalf of a provider with a differen peer ID.
	Allow bool
	// Except is a list of peer IDs that are exceptions to the Allow
	// policy.
	Except []string
}

// NewPolicy returns Policy with values set to their defaults.
func NewPolicy() Policy {
	return Policy{
		Allow: true,
	}
}
