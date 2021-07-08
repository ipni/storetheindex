package importer

import (
	"bufio"
	"context"
	"io"

	"github.com/ipfs/go-cid"
)

// CidListImporter reads from a list of CIDs.
type CidListImporter struct {
	reader io.Reader
}

func NewCidListImporter(r io.Reader) Importer {
	return CidListImporter{r}
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
