// Package dskey holds the keys and values that the indexer writes to its
// datastore and that more than one package reads: the ingester's sync state
// and ad markers, the registry's provider info and the freezer's frozen
// state. They are a persisted on-disk format.
package dskey

const (
	// SyncPrefix starts the key of each publisher's latest synced ad:
	// SyncPrefix + publisher ID, with the ad's CID bytes as the value
	SyncPrefix = "/sync/"
	// AdProcessedPrefix starts the key of each ad's marker: AdProcessedPrefix
	// + ad CID, with one of the AdMarker bytes as the value
	AdProcessedPrefix = "/adProcessed/"
	// AdSkipReasonPrefix starts the key of the skip reason of each
	// permanently skipped ad: AdSkipReasonPrefix + ad CID
	AdSkipReasonPrefix = "/adSkipReason/"
	// ProviderInfoPath is the parent key of the provider info records:
	// ProviderInfoPath + "/" + provider ID
	ProviderInfoPath = "/registry/pinfo"
	// Frozen is the key that is present when the indexer is frozen
	Frozen = "/freeze/frozen"
)

// Marker byte values of an /adProcessed/<cid> key.
//
// The stored value is the marker byte, optionally followed by 8 bytes
// holding little-endian microseconds since the Unix epoch recording when the
// marker was last written. Legacy records are marker-only (1 byte); current
// writes of processed and skipped markers are 9 bytes. Readers accept
// trailing bytes after the timestamp.
const (
	AdMarkerUnprocessed byte = 0 // unprocessed
	AdMarkerProcessed   byte = 1 // processed successfully
	AdMarkerResync      byte = 2 // marked for resync
	AdMarkerSkipped     byte = 3 // processed but permanently skipped
)
