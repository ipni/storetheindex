package command

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	agg "github.com/filecoin-project/go-dagaggregator-unixfs"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/urfave/cli/v2"
)

const DAG_MAX = 3200000

var syntheticCmd = &cli.Command{
	Name:   "synthetic",
	Usage:  "Generate synthetic load to import in indexer",
	Flags:  syntheticFlags,
	Action: syntheticAction,
}

var syntheticFlags = []cli.Flag{
	fileFlag,
	&cli.StringFlag{
		Name:     "type",
		Usage:    "Type of synthetic load to generate (manifest, cidlist, car)",
		Aliases:  []string{"t"},
		Required: true,
	},
	&cli.Int64Flag{
		Name:     "num",
		Usage:    "Number of entries to generate",
		Aliases:  []string{"n"},
		Required: false,
	},
	&cli.Int64Flag{
		Name:     "size",
		Usage:    "Total size of the CIDs to generate",
		Aliases:  []string{"s"},
		Required: false,
	},
}

func syntheticAction(c *cli.Context) error {
	fileName := c.String("file")
	num := int(c.Int64("num"))
	size := int(c.Int64("size"))
	t := c.String("type")

	if num == 0 && size == 0 {
		return errors.New("no size or number of cids provided to command")
	}

	switch t {
	case "manifest":
		return genManifest(fileName, num, size)
	case "cidlist":
		return genCidList(fileName, num, size)
	}
	return errors.New("export type not implemented, try types manifest or cidlist")
}

func genCidList(fileName string, num int, size int) error {
	fmt.Println("Generating cidlist file")
	if size != 0 {
		return writeCidFileOfSize(fileName, size)
	}
	return writeCidFile(fileName, num)
}

func genManifest(fileName string, num int, size int) error {
	fmt.Println("Generating manifest file")
	if size != 0 {
		return writeManifestOfSize(fileName, size)
	}
	return writeManifest(fileName, num)
}

type progress struct {
	count int
	incr  float64
	next  float64
}

func newProgress(total int) *progress {
	const percentIncr = 2

	fmt.Fprintln(os.Stderr, "|         25%|        50%|         75%|        100%|")
	fmt.Fprint(os.Stderr, "|")
	incr := float64(total*percentIncr) / 100.0
	return &progress{
		incr:  incr,
		next:  incr,
		count: 100 / percentIncr,
	}
}

func (p *progress) update(curr int) {
	for p.count > 0 && float64(curr) >= p.next {
		fmt.Fprint(os.Stderr, ".")
		p.next += p.incr
		p.count--
	}
}

func (p *progress) done() {
	for p.count > 0 {
		fmt.Fprint(os.Stderr, ".")
		p.count--
	}
	fmt.Fprintln(os.Stderr, "|")
}

// writeCidFile creates a file and appends a list of cids.
func writeCidFile(fileName string, num int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)

	prog := newProgress(num)

	var cids []cid.Cid
	var curr, i int
	for curr < num {
		if i == len(cids) {
			// Refil cids
			cids = random.Cids(100)
			i = 0
		}
		if _, err = w.WriteString(cids[i].String()); err != nil {
			return err
		}
		if _, err = w.WriteString("\n"); err != nil {
			return err
		}
		curr++
		i++
		prog.update(curr)
	}

	if err = w.Flush(); err != nil {
		return err
	}
	prog.done()

	fmt.Println("Created cidList successfully")
	return nil
}

// writeCidFileOfSize creates a new file of a specific size
func writeCidFileOfSize(fileName string, size int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)

	prog := newProgress(size)

	var cids []cid.Cid
	var curr, i int
	for curr < size {
		if i == len(cids) {
			// Refil cids
			cids = random.Cids(100)
			i = 0
		}
		c := cids[i]
		i++
		if _, err = w.WriteString(c.String()); err != nil {
			return err
		}
		if _, err = w.WriteString("\n"); err != nil {
			return err
		}
		curr += len(c.Bytes())
		prog.update(curr)
	}

	if err = w.Flush(); err != nil {
		return err
	}
	prog.done()

	fmt.Println("Created cidList successfully of size:", size)
	return nil
}

// writeManifest appends new entries to existing manifest
func writeManifest(fileName string, num int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)

	prog := newProgress(num)

	var cids []cid.Cid
	var curr, i int
	for curr < num {
		if i == len(cids) {
			// Refil cids
			cids = random.Cids(100)
			i = 0
		}

		b, err := manifestEntry(cids[i])
		if err != nil {
			return err
		}
		if _, err = w.Write(b); err != nil {
			return err
		}
		if _, err = w.WriteString("\n"); err != nil {
			return err
		}
		i++
		curr++
		prog.update(curr)
	}

	if err = w.Flush(); err != nil {
		return err
	}
	prog.done()

	fmt.Println("Created Manifest successfully")
	return nil
}

// writeManifestOfSize creates a manifest for certain size of CIDs
func writeManifestOfSize(fileName string, size int) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)

	prog := newProgress(size)

	var cids []cid.Cid
	var curr, i int
	for curr < size {
		if i == len(cids) {
			// Refil cids
			cids = random.Cids(100)
			i = 0
		}
		c := cids[i]
		i++
		b, err := manifestEntry(c)
		if err != nil {
			return err
		}
		if _, err = w.Write(b); err != nil {
			return err
		}
		if _, err = w.WriteString("\n"); err != nil {
			return err
		}
		curr += len(c.Bytes())
		prog.update(curr)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	prog.done()

	fmt.Println("Created Manifest successfully")
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
