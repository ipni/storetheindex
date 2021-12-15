package config

const (
	defaultIngestPubSubTopic = "/indexer/ingest/mainnet"
	defaultStoreBatchSize    = 256
)

// Ingest tracks the configuration related to the ingestion protocol.
type Ingest struct {
	// PubSubTopic used to advertise ingestion announcements.
	PubSubTopic string
	// PubSubPeers is a list of peer IDs to allow pubsub messages to originate from.
	PubSubPeers []string
	// StoreBatchSize is the number of entries in each write to the value
	// store.  Specifying a value less than 2 disables batching.
	StoreBatchSize int
}
