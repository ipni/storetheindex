package importer

import (
	"bufio"
	"context"
	"os"

	"github.com/ipfs/go-cid"
)

// CidListImporter reads from a list of CIDs.
type CidListImporter struct {
	dir string
}

func NewCidListImporter(dir string) Importer {
	return CidListImporter{dir}
}

func (i CidListImporter) Read(ctx context.Context, out chan cid.Cid, done chan error) {
	defer close(out)
	defer close(done)

	file, err := os.Open(i.dir)
	if err != nil {
		done <- err
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			c, err := cid.Decode(scanner.Text())
			if err != nil {
				// Disregarding malformed CIDs for now
				continue
			}
			out <- c
		}
	}

	if err := scanner.Err(); err != nil {
		done <- err
		return
	}
	return
}
