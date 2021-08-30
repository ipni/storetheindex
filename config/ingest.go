package config

const (
	defaultIngestPubSubTopic = "indexer/ingest"
)

// Ingest tracks the configuration related to the ingestion protocol
type Ingest struct {
	// PubSubTopic used to advertise ingestion announcements.
	PubSubTopic string
}
