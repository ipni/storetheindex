package v0

import "github.com/libp2p/go-libp2p-core/protocol"

const (
	// FinderProtocolID is the libp2p protocol that finder API uses
	FinderProtocolID protocol.ID = "/indexer/finder/0.0.1"
	// FinderProtocolID is the libp2p protocol that ingest API uses
	IngestProtocolID protocol.ID = "/indexer/indest/0.0.1"
)
