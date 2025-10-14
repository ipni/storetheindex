package config

import (
	"time"

	sticfg "github.com/ipni/storetheindex/config"
)

// Assignment holds addresses of indexers to assign publishers to, policy
// specifying which peers to allow announce messages from, and related
// settings.
type Assignment struct {
	// FilterIPs, when true, removes any private, loopback, or unspecified IP
	// addresses from provider and publisher addresses.
	FilterIPs bool
	// PoolInterval is how often to poll indexers for status.
	PollInterval sticfg.Duration
	// IndexerPool is the set of indexers the pool.
	IndexerPool []Indexer
	// Policy configures which peers are allowed and blocked.
	Policy Policy
	// PubSubTopic sets the topic name to which to subscribe for ingestion
	// announcements.
	PubSubTopic string
	// PresetReplication is the number of pre-assigned indexers to assign a
	// publisher to. See Indexer.PresetPeers. Any value < 1 defaults to 1.
	PresetReplication int
	// Replication is the number of indexers to assign each publisher to, when
	// the publisher does not have a preset assignment. A value <= 0 assigns
	// each publisher to one indexer.
	Replication int
	// ResendHttp, if true, resends announcements directly to assigned insexers.
	ResendHttp bool
	// ResendPubsub, if true, resends announcements over libp2p pubsub.
	ResendPubsub bool
}

type Indexer struct {
	// AdminURL is the base URL for the indexer's admin interface.
	AdminURL string
	// FindURL is the base URL for the indexer's find interface.
	FindURL string
	// IngestURL is the base URL for the indexer's ingest interface.
	IngestURL string
	// PresetPeers is a list of the peer IDs of pre-assigned publishers. A
	// publisher is assigned to n of the indexers that has the publisher in
	// PresetPeers, where n is PresetReplication.
	PresetPeers []string
}

func NewIndexer() Indexer {
	return Indexer{}
}

// NewDiscovery returns Discovery with values set to their defaults.
func NewAssignment() Assignment {
	return Assignment{
		PollInterval:      sticfg.Duration(5 * time.Minute),
		Policy:            NewPolicy(),
		PubSubTopic:       "/indexer/ingest/mainnet",
		PresetReplication: 1,
		Replication:       1,
		ResendHttp:        true,
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Assignment) populateUnset() {
	def := NewAssignment()

	if c.PollInterval == 0 {
		c.PollInterval = def.PollInterval
	}
	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
	if c.PresetReplication <= 0 {
		c.PresetReplication = def.PresetReplication
	}
	if c.Replication <= 0 {
		c.Replication = def.Replication
	}
}
