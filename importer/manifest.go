package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"os"

	agg "github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/ipfs/go-cid"
)

// MenifestImporter reads Cids from a manifest of a  CID aggregator
type ManifestImporter struct {
	dir string
}

func NewManifestImporter(dir string) Importer {
	return ManifestImporter{dir}
}

func (i ManifestImporter) Read(ctx context.Context, out chan cid.Cid, done chan error) {
	defer close(out)
	defer close(done)
	file, err := os.Open(i.dir)
	if err != nil {
		done <- err
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	// optionally, resize scanner's capacity for lines over 64K
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			e := agg.ManifestDagEntry{}
			// In its current implementation, there is no performance benefit
			// from using json.Unmarshal v.s. json.Decode, as Decode uses a
			// buffer to unmarshal, so they have similar performance.
			// This will change in future versions of Go, evaluate then
			// if it makes sense to change the implementation.
			err := json.Unmarshal(scanner.Bytes(), &e)
			// Disregard lines that can't be unmarshalled
			if err != nil {
				continue
			}
			// Check if DagEntry
			if e.RecordType == "DagAggregateEntry" {
				// Using original CID and not the normalized Cid. We could choose
				// to read both (althoug it is not needed)
				c, err := cid.Decode(e.DagCid)
				// There shouldn't be malformed CIDs, if there are just
				// disregard them
				if err != nil {
					continue
				}
				out <- c

			}
		}
	}

	if err := scanner.Err(); err != nil {
		done <- err
		return
	}
	return
}
