package store

import (
	"encoding/json"

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
	Get(c cid.Cid) ([]IndexEntry, bool, error)
	// Put info for CID in primary storage.
	Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error
	// Bulk put in primary storage
	// NOTE: The interface may change if we see some other function
	// signature more handy for updating process.
	PutMany(cs []cid.Cid, provID peer.ID, pieceID cid.Cid) error
	// Close or flush storage to commit changes
	Close()
	// Size returns the total storage capacity being used
	Size() (int64, error)
}

// Marshal serializes IndexEntry list for storage
// TODO: Switch from JSON to a more efficient serialization
// format once we figure out the right data structure?
func Marshal(li []IndexEntry) ([]byte, error) {
	return json.Marshal(&li)
}

// Unmarshal serialized IndexEntry list
func Unmarshal(b []byte) ([]IndexEntry, error) {
	li := []IndexEntry{}
	err := json.Unmarshal(b, &li)
	return li, err
}

// DuplicateEntry checks if the entry already exists in the index. An entry
// for the same provider but a different piece is not considered
// a duplicate entry (at least for now)
func DuplicateEntry(in IndexEntry, old []IndexEntry) bool {
	for i := range old {
		if in.PieceID == old[i].PieceID &&
			in.ProvID == old[i].ProvID {
			return true
		}
	}
	return false
}
