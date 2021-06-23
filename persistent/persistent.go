package persistent

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// Work in progress
type Store interface {
	Get(c cid.Cid) (blocks.Block, error)
	Put(blk blocks.Block) error
	PutMany(blks []blocks.Block) error
}

func New() Store {
	panic("not implemented")
}
