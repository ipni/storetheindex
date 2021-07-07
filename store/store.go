package store

import (
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// IndexEntry describes the information to be stored for each CID in the indexer.
type IndexEntry struct {
	ProvID  peer.ID // ID of the provider of the CID.
	PieceID cid.Cid // PieceID of the CID where the CID is stored in the provider (may be nil)
}

// Storage is the main interface for storage systems used in the indexer.
// NOTE: Peristent and primary storage implementations currently share the
// same interface. This may change in the future if we want to discern between
// them more easily, or if we want to introduce additional features to either of them.
type Storage interface {
	// Get info for CID from primary storage.
	Get(c cid.Cid) ([]IndexEntry, bool)
	// Put info for CID in primary storage.
	Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error
	// Bulk put in primary storage
	// NOTE: The interface may change if we see some other function
	// signature more handy for updating process.
	PutMany(cs []cid.Cid, provID peer.ID, pieceID cid.Cid) error
	// Remove info for CID
	Remove(c cid.Cid, provID peer.ID, pieceID cid.Cid) bool
}
