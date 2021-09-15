package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"io"

	agg "github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// ReadManifest reads Cids from a manifest of a CID aggregator and outputs
// their multihashes on a channel.
func ReadManifest(ctx context.Context, in io.Reader, out chan<- multihash.Multihash, errOut chan error) {
	defer close(errOut)

	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		e := agg.ManifestDagEntry{}
		// In its current implementation, there is no performance benefit
		// from using json.Unmarshal v.s. json.Decode, as Decode uses a
		// buffer to unmarshal, so they have similar performance.
		// This will change in future versions of Go, evaluate then
		// if it makes sense to change the implementation.
		err := json.Unmarshal(scanner.Bytes(), &e)
		if err != nil {
			continue
		}
		// Check if DagEntry
		if e.RecordType == "DagAggregateEntry" {
			// Using original CID and not the normalized Cid. We could choose
			// to read both (althoug it is not needed)
			c, err := cid.Decode(e.DagCidV1)
			if err != nil {
				c, err = cid.Decode(e.DagCidV0)
				if err != nil {
					continue // ignore malformet CIDs
				}
			}
			if !c.Defined() {
				continue
			}
			select {
			case out <- c.Hash():
			case <-ctx.Done():
				close(out) // close out first in case errOut not buffered
				errOut <- ctx.Err()
				return
			}
		} else if ctx.Err() != nil {
			close(out) // close out first in case errOut not buffered
			errOut <- ctx.Err()
			return
		}
	}
	// Close out first in case errOut is not buffered, to let the caller's
	// range loop exit and then read errOut
	close(out)

	if err := scanner.Err(); err != nil {
		errOut <- err
	}
}
