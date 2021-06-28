package commands

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	agg "github.com/filecoin-project/go-dagaggregator-unixfs"
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
	num := c.Int("num")
	t := c.String("type")
	switch t {
	case "manifest":
		return genManifest(dir, num)
	case "cidlist":
		return genCidList(dir, num)
	default:
		return fmt.Errorf("Export type not implemented, try types manifest or cidlist")
	}

}

func genCidList(dir string, num int) error {
	log.Infow("Starting to synthetize cidlist file")
	cids, _ := randomCids(num)
	return writeCidFile(dir, cids)
}
func genManifest(dir string, num int) error {
	log.Infow("Starting to synthetize manifest file")
	cids, _ := randomCids(num)
	return writeManifest(dir, cids)
}

// Prefix used for CIDs.
// NOTE: Consider using several formats in the future.
var pref = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   mh.SHA2_256,
	MhLength: -1, // default length
}

func randomCids(n int) ([]cid.Cid, error) {
	rand.Seed(time.Now().UnixNano())
	res := make([]cid.Cid, 0)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		rand.Read(b)
		c, err := pref.Sum(b)
		if err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, nil

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
		file.WriteString(c.String() + "\n")
	}
	log.Infof("Created cidList successfully")
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

	// NOTE: We are not including ManifestPreamble and Summary,
	// as for importing purposes we only use DagEntries. We are also
	// not setting some of the fields because we currently don't use them
	// for import. Set them conveniently if neccessary.
	for _, c := range cids {
		n := uint64(1)
		e := agg.ManifestDagEntry{
			RecordType:    "DagAggregateEntry",
			DagCid:        c.String(),
			NormalizedCid: c.String(),
			NodeCount:     &n,
		}
		b, _ := json.Marshal(e)
		file.WriteString(string(b) + "\n")
	}
	log.Infof("Created Manifest successfully")
	return nil
}
