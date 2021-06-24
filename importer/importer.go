package importer

import (
	"context"

	"github.com/ipfs/go-cid"
)

// Importer exposes an importer interface to be used for any source.
type Importer interface {
	Read(ctx context.Context, out chan cid.Cid, done chan error)
}
