package config

// Assignment holds addresses of indexers to assign publishers to, policy
// specifying which peers to allow announce messages from, and related
// settings.
type Assignment struct {
	// FilterIPs, when true, removes any private, loopback, or unspecified IP
	// addresses from provider and publisher addresses.
	FilterIPs bool
	// IndexerPool is the set of indexers the pool.
	IndexerPool []Indexer
	// Policy configures which peers are allowed and blocked.
	Policy Policy
	// PubSubTopic sets the topic name to which to subscribe for ingestion
	// announcements.
	PubSubTopic string
	// Replication is the number of indexers to assign each publisher to. If
	// set to 0, the default, then assign to all indexers.
	Replication int
}

type Indexer struct {
	AdminURL  string
	FindURL   string
	IngestURL string
}

func NewIndexer() Indexer {
	return Indexer{}
}

// NewDiscovery returns Discovery with values set to their defaults.
func NewAssignment() Assignment {
	return Assignment{
		Policy:      NewPolicy(),
		PubSubTopic: "/indexer/ingest/mainnet",
	}
}

// populateUnset replaces zero-values in the config with default values.
func (c *Assignment) populateUnset() {
	def := NewAssignment()

	if c.PubSubTopic == "" {
		c.PubSubTopic = def.PubSubTopic
	}
}
