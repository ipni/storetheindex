package importer

import (
	"bufio"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// CidListImporter reads from a list of CIDs.
type CidListImporter struct {
	reader io.Reader
	miner  peer.ID
}

func NewCidListImporter(r io.Reader, miner peer.ID) Importer {
	return CidListImporter{r, miner}
}

func (i CidListImporter) Read(ctx context.Context, out chan cid.Cid, done chan error) {
	defer close(out)
	defer close(done)

	r := bufio.NewReader(i.reader)
	var line []byte
	var err error
	for {
		line, err = r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				done <- err
				return
			} else {
				return
			}
		}
		select {
		case <-ctx.Done():
			done <- err
			return
		default:
			c, err := cid.Decode(string(line))
			if err != nil {
				// Disregarding malformed CIDs for now
				continue
			}
			out <- c
		}
	}
}
