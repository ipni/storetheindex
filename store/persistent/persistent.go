package persistent

import (
	"github.com/adlrocha/indexer-node/store"
	sth "github.com/hannahhoward/go-storethehash"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var _ store.Storage = &sthStorage{}

// Work in progress
type sthStorage struct {
	// TODO: Use HashedBlockstore or build our index storage over
	// the storethehash.store.
	store *sth.HashedBlockstore
}

func New() store.Storage {
	// TODO: Not implemented
	return nil
}

func (s *sthStorage) Get(c cid.Cid) ([]store.IndexEntry, bool) {
	panic("Not implemented")
}
func (s *sthStorage) Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	panic("Not implemented")
}
func (s *sthStorage) PutMany(cs []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	panic("Not implemented")
}
