package importer

import (
	"bufio"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// ReadCids reads cids from an io.Reader and output their multihashes on a
// channel.  Malformed cids are ignored.  ReadCids is meant to be called in a
// separate goroutine. It exits when EOF on in io.Reader or when context
// caceled.
func ReadCids(ctx context.Context, in io.Reader, out chan<- multihash.Multihash, done chan error) {
	defer close(out)
	defer close(done)

	r := bufio.NewReader(in)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				done <- err
			}
			return
		}
		c, err := cid.Decode(line)
		if err != nil || !c.Defined() {
			// Ignore malformed CIDs
			continue
		}
		select {
		case out <- c.Hash():
		case <-ctx.Done():
			done <- ctx.Err()
			return
		}
	}
}
