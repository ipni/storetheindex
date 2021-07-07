package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	agg "github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/filecoin-project/storetheindex/utils"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

const DAG_MAX = 3200000

var SyntheticCmd = &cli.Command{
	Name:   "synthetic",
	Usage:  "Generate synthetic load to import in indexer",
	Flags:  SyntheticFlags,
	Action: syntheticCmd,
}

func syntheticCmd(c *cli.Context) error {
	dir := c.String("dir")
	num := c.Int64("num")
	size := c.Int64("size")
	t := c.String("type")
	switch t {
	case "manifest":
		return genManifest(dir, num, size)
	case "cidlist":
		return genCidList(dir, num, size)
	default:
		return fmt.Errorf("Export type not implemented, try types manifest or cidlist")
	}

}

func genCidList(dir string, num int64, size int64) error {
	log.Infow("Starting to synthetize cidlist file")
	if size != 0 {
		return writeCidFileOfSize(dir, size)
	} else if num != 0 {
		cids, _ := utils.RandomCids(int(num))
		return writeCidFile(dir, cids)
	}

	return fmt.Errorf("No size or number of cids provided to command")

}

func genManifest(dir string, num int64, size int64) error {
	log.Infow("Starting to synthetize manifest file")
	if size != 0 {
		return writeManifestOfSize(dir, size)
	} else if num != 0 {
		cids, _ := utils.RandomCids(int(num))
		return writeManifest(dir, cids)
	}
	return fmt.Errorf("No size or number of cids provided to command")
}

// Prefix used for CIDs.
// NOTE: Consider using several formats in the future.
var pref = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1, // default length
}

// writeCidFile creates a file and appends a list of cids.
// If the file already exists it appends to it to benefit
// from previous runs (this can make us save time).
func writeCidFile(dir string, cids []cid.Cid) error {
	// file, err := os.Create(dir)
	file, err := os.OpenFile(dir,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, c := range cids {
		_, err = file.WriteString(c.String() + "\n")
		if err != nil {
			return err
		}
	}
	log.Infof("Created cidList successfully")
	return nil
}

// writeCidFileOfSize creates a new file of a specific size
func writeCidFileOfSize(dir string, size int64) error {
	file, err := os.OpenFile(dir,
		os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	curr := int64(0)
	for curr < size {
		// Generate CIDs in batches of 1000
		cids, _ := utils.RandomCids(1000)
		for _, c := range cids {
			curr += int64(len(c.Bytes()))
			_, err = file.WriteString(c.String() + "\n")
			if err != nil {
				return err
			}
			progress(curr)
		}

	}
	log.Infof("Created cidList successfully of size: %d", size)
	return nil
}

// writeManifest appends new entries to existing manifest
// If the file already exists it appends to it to benefit
// from previous runs (this can make us save time).
func writeManifest(dir string, cids []cid.Cid) error {
	// file, err := os.Create(dir)
	file, err := os.OpenFile(dir,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, c := range cids {
		b, err := manifestEntry(c)
		if err != nil {
			return err
		}
		_, err = file.WriteString(string(b) + "\n")
		if err != nil {
			return err
		}
	}
	log.Infof("Created Manifest successfully")
	return nil
}

// writeManifestOfSize creates a manifest for certain size of CIDs
func writeManifestOfSize(dir string, size int64) error {
	file, err := os.OpenFile(dir,
		os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	curr := int64(0)
	for curr < size {
		// Generate CIDs in batches of 1000
		cids, _ := utils.RandomCids(1000)
		for _, c := range cids {
			curr += int64(len(c.Bytes()))
			b, err := manifestEntry(c)
			if err != nil {
				return err
			}
			_, err = file.WriteString(string(b) + "\n")
			if err != nil {
				return err
			}
			progress(curr)
		}

	}
	log.Infof("Created Manifest successfully")
	return nil
}

func manifestEntry(c cid.Cid) ([]byte, error) {
	// NOTE: We are not including ManifestPreamble and Summary,
	// as for importing purposes we only use DagEntries. We are also
	// not setting some of the fields because we currently don't use them
	// for import. Set them conveniently if neccessary.
	n := uint64(1)
	e := agg.ManifestDagEntry{
		RecordType: "DagAggregateEntry",
		NodeCount:  &n,
	}
	switch c.Version() {
	case 1:
		e.DagCidV1 = c.String()
	case 0:
		e.DagCidV0 = c.String()
	default:
		return nil, errors.New("unsupported cid version")
	}

	return json.Marshal(e)
}

func progress(n int64) {
	if n%50000 == 0 {
		log.Infof("Generated %dB so far", n)
	}
}
