package store

import (
	"bytes"
	"encoding/json"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	varint "github.com/multiformats/go-varint"
)

// IndexEntry describes the information to be stored for each CID in the indexer.
type IndexEntry struct {
	// PrividerID is the peer ID of the provider of the CID
	ProviderID peer.ID
	// Metadata is serialized data that provides information about retrieving
	// data, for the indexed CID, from the identified provider.
	Metadata []byte
}

func MakeIndexEntry(providerID peer.ID, protocol uint64, data []byte) IndexEntry {
	return IndexEntry{
		ProviderID: providerID,
		Metadata:   encodeMetadata(protocol, data),
	}
}

// PutData writes the protocol ID and the encoded data to IndexEntry.Metadata
func (ie *IndexEntry) PutData(protocol uint64, data []byte) {
	ie.Metadata = encodeMetadata(protocol, data)
}

// GetData returns the protocol ID and the encoded data from the IndexEntry.Metadata
func (ie *IndexEntry) GetData() (uint64, []byte, error) {
	protocol, len, err := varint.FromUvarint(ie.Metadata)
	if err != nil {
		return 0, nil, err
	}
	return protocol, ie.Metadata[len:], nil
}

// Equal returns true if two IndexEntry instances are identical
func (ie IndexEntry) Equal(other IndexEntry) bool {
	return ie.ProviderID == other.ProviderID && bytes.Equal(ie.Metadata, other.Metadata)
}

func encodeMetadata(protocol uint64, data []byte) []byte {
	varintSize := varint.UvarintSize(protocol)
	buf := make([]byte, varintSize+len(data))
	varint.PutUvarint(buf, protocol)
	if len(data) != 0 {
		copy(buf[varintSize:], data)
	}
	return buf
}

// Storage is the main interface for storage systems used in the indexer.
// NOTE: Peristent and primary storage implementations currently share the
// same interface. This may change in the future if we want to discern between
// them more easily, or if we want to introduce additional features to either of them.
type Storage interface {
	// Get retrieves a slice of IndexEntry for a CID
	Get(c cid.Cid) ([]IndexEntry, bool, error)
	// Put stores an additional IndexEntry for a CID if the entry is not already stored
	Put(c cid.Cid, entry IndexEntry) error
	// PutMany stores an IndexEntry for multiple CIDs
	PutMany(cs []cid.Cid, entry IndexEntry) error
	// Remove removes an IndexEntry for a CID
	Remove(c cid.Cid, entry IndexEntry) error
	// RemoveMany removes an IndexEntry from multiple CIDs
	RemoveMany(cids []cid.Cid, entry IndexEntry) error
	// RemoveProvider removes all entries for specified provider.  This is used
	// when a provider is no longer indexed by the indexer.
	RemoveProvider(providerID peer.ID) error
	// Size returns the total storage capacity being used
	Size() (int64, error)
}

// StorageFlusher implements a storage interface with Flush capabilities
// to be used with persistence storage that require commitment of changes
// on-disk.
type StorageFlusher interface {
	Storage
	// Flush commits changes to storage
	Flush() error
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
