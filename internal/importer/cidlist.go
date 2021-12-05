package importer

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("indexer/importer")

// ReadCids reads cids from an io.Reader and output their multihashes on a
// channel.  Malformed cids are ignored.  ReadCids is meant to be called in a
// separate goroutine. It exits when EOF on in io.Reader or when context
// caceled.
func ReadCids(ctx context.Context, in io.Reader, out chan<- multihash.Multihash, done chan error) {
	defer close(out)
	defer close(done)

	var badEntryCount, entryCount int
	r := bufio.NewReader(in)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				done <- err
				return
			}
			break
		}
		c, err := cid.Decode(line)
		if err != nil || !c.Defined() {
			badEntryCount++
			// Ignore malformed CIDs
			continue
		}
		select {
		case out <- c.Hash():
			entryCount++
		case <-ctx.Done():
			done <- ctx.Err()
			return
		}
	}
	if badEntryCount != 0 {
		log.Errorf("Skipped %d bad cid entries", badEntryCount)
	}
	if entryCount == 0 {
		done <- errors.New("no entries imported")
		return
	}
	log.Infof("Imported %d cid entries", entryCount)
}
