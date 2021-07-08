package persistent

import (
	"github.com/filecoin-project/storetheindex/store"
	"github.com/ipfs/go-cid"
	sth "github.com/ipld/go-storethehash"
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

func (s *sthStorage) Get(c cid.Cid) ([]store.IndexEntry, bool, error) {
	panic("Not implemented")
}

func (s *sthStorage) Put(c cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	panic("Not implemented")
}

func (s *sthStorage) PutMany(cids []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	panic("Not implemented")
}

func (s *sthStorage) Remove(c cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	panic("Not implemented")
}

func (s *sthStorage) RemoveMany(cids []cid.Cid, provID peer.ID, pieceID cid.Cid) error {
	panic("Not implemented")
}

func (s *sthStorage) RemoveProvider(providerID peer.ID) error {
	panic("Not implemented")
}
