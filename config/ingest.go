package config

const (
	defaultIngestPubSubTopic = "indexer/ingest"
	defaultStoreBatchSize    = 64
)

// Ingest tracks the configuration related to the ingestion protocol.
type Ingest struct {
	// PubSubTopic used to advertise ingestion announcements.
	PubSubTopic string
	// PubSubPeer is the libp2p peer ID of the host to subscribe to.
	PubSubPeer string
	// StoreBatchSize is the number of entries in each write to the value
	// store.  Specifying a value less than 2 disables batching.
	StoreBatchSize int
}
