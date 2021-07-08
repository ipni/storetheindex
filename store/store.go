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
	// Get retrieves provider-piece info for a CID
	Get(c cid.Cid) ([]IndexEntry, bool)
	// Put stores a provider-piece entry for a CID if the entry is
	// not already stored.  New entries are added to the entries that are already
	// there.  Returns true if a new entry was added to the cache.
	Put(c cid.Cid, providerID peer.ID, pieceID cid.Cid) bool
	// PutMany stores the provider-piece entry for multiple CIDs.  Returns the
	// number of new entries stored.
	PutMany(cs []cid.Cid, providerID peer.ID, pieceID cid.Cid) int
	// Remove removes a provider-piece entry for a CID
	Remove(c cid.Cid, providerID peer.ID, pieceID cid.Cid) bool
	// RemoveMany removes a provider-piece entry from multiple CIDs.  Returns
	// the number of entries removed.
	RemoveMany(cids []cid.Cid, providerID peer.ID, pieceID cid.Cid) int
	// RemoveProvider removes all enrties for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(providerID peer.ID) int
}
