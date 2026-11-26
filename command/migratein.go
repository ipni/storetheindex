package command

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	pbl "github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/storetheindex/internal/migrate"
	"github.com/urfave/cli/v2"
)

// headerTimeout is how long the import waits for a part's headers
const headerTimeout = time.Minute

var MigrateInCmd = &cli.Command{
	Name:  "migrate-in",
	Usage: "Import another indexer's data into this indexer's stores",
	Description: "Imports the data of an existing indexer, available at --source-url, into this " +
		"indexer's datastore at --datastore-dir and value store at --valuestore-dir. Stop every service " +
		"that uses these stores first. Run the command again to resume an import that stopped.",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "source-url",
			Usage:    "Base URL of the existing indexer to import from",
			Required: true,
		},
		&cli.PathFlag{
			Name:     "token-file",
			Usage:    "File that holds the source's import token",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "datastore-type",
			Usage: "Type of this indexer's datastore, as Datastore.Type in its config: levelds",
			Value: "levelds",
		},
		&cli.PathFlag{
			Name:     "datastore-dir",
			Usage:    "Directory of this indexer's datastore",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "valuestore-type",
			Usage: "Type of this indexer's value store, as Indexer.ValueStoreType in its config: pebble",
			Value: "pebble",
		},
		&cli.PathFlag{
			Name:     "valuestore-dir",
			Usage:    "Directory of this indexer's value store",
			Required: true,
		},
		&cli.PathFlag{
			Name:  "valuestore-options",
			Usage: "Options file that the value store is opened with, so it is written with the same options: for pebble, its options file",
		},
		&cli.BoolFlag{
			Name: "validate-records",
			Usage: "Delete the records left behind by data that changed on the source during the import. " +
				"It reads every record and can take hours on a large store; without it, those records " +
				"stay until a later cleanup. Turning it off part way through skips the rest of it.",
			Value: true,
		},
		&cli.IntFlag{
			Name:  "workers",
			Usage: "Number of parts copied at once",
			Value: importer.DefaultWorkers,
		},
		&cli.DurationFlag{
			Name:  "worker-stagger",
			Usage: "Time between the starts of new workers",
			Value: importer.DefaultStagger,
		},
		&cli.DurationFlag{
			Name:  "retry-window",
			Usage: "How long a part may keep failing before the import stops",
			Value: importer.DefaultRetryWindow,
		},
	},
	Action: migrateInAction,
}

func migrateInAction(cctx *cli.Context) error {
	if n := cctx.Int("workers"); n < 0 || n > importer.MaxWorkers {
		return fmt.Errorf("--workers must be from 0 to %d", importer.MaxWorkers)
	}

	b, err := os.ReadFile(cctx.Path("token-file"))
	if err != nil {
		return fmt.Errorf("cannot read the import token: %w", err)
	}

	token := strings.TrimSpace(string(b))
	if token == "" {
		return errors.New("the import token file is empty")
	}

	src, err := importhttp.NewClient(
		cctx.String("source-url"),
		token,
		importhttp.WithHTTPClient(newImportHTTPClient(cctx.Int("workers"))),
	)
	if err != nil {
		return err
	}

	ds, err := openMigrateDatastore(cctx.String("datastore-type"), cctx.Path("datastore-dir"))
	if err != nil {
		return err
	}
	defer func() { _ = ds.Close() }()

	vs, err := openMigrateValueStore(cctx.String("valuestore-type"), cctx.Path("valuestore-dir"), cctx.Path("valuestore-options"))
	if err != nil {
		return err
	}
	defer func() { _ = vs.Close() }()

	log.Infow("Importing", "source", cctx.String("source-url"))
	err = migrate.Run(cctx.Context, ds, src, vs.(importer.Target), cctx.Bool("validate-records"),
		importer.WithWorkers(cctx.Int("workers")),
		importer.WithStagger(cctx.Duration("worker-stagger")),
		importer.WithRetryWindow(cctx.Duration("retry-window")))
	if err != nil {
		return fmt.Errorf("the import stopped; run it again to resume: %w", err)
	}

	log.Info("Import done")
	return nil
}

// newImportHTTPClient returns the HTTP client of an import with workers
// workers. It speaks HTTP/1.1 only, so that each part in flight has a TCP
// connection of its own: HTTP/2 would carry every part over one connection,
// whose congestion window would limit them all. It keeps a connection per
// worker between parts.
func newImportHTTPClient(workers int) *http.Client {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	return &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ResponseHeaderTimeout: headerTimeout,
		MaxIdleConnsPerHost:   workers,
		Protocols:             &protocols,
	}}
}

// openMigrateDatastore opens the datastore of type dsType at dir, which an
// import writes into
func openMigrateDatastore(dsType, dir string) (datastore.Batching, error) {
	switch dsType {
	case "levelds":
		ds, err := leveldb.NewDatastore(dir, nil)
		if err != nil {
			return nil, fmt.Errorf("cannot open the datastore: %w", err)
		}
		return ds, nil
	default:
		return nil, fmt.Errorf("an import cannot write a datastore of type %q; supported: levelds", dsType)
	}
}

// openMigrateValueStore opens the value store of type vsType at dir, with
// the options file at optionsPath when it is not empty, which an import
// writes into
func openMigrateValueStore(vsType, dir, optionsPath string) (indexer.Interface, error) {
	switch vsType {
	case "pebble":
		opts, err := readPebbleOptions(optionsPath)
		if err != nil {
			return nil, err
		}
		vs, err := pebble.New(dir, opts)
		if err != nil {
			return nil, fmt.Errorf("cannot open the value store: %w", err)
		}
		return vs, nil
	default:
		return nil, fmt.Errorf("an import cannot write a value store of type %q; supported: pebble", vsType)
	}
}

// readPebbleOptions returns pebble options with the defaults, and with the
// options file at path when it is not empty
func readPebbleOptions(path string) (*pbl.Options, error) {
	opts := &pbl.Options{}
	opts.EnsureDefaults()
	if path == "" {
		return opts, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read the pebble options: %w", err)
	}
	err = opts.Parse(string(b), &pbl.ParseHooks{
		NewFilterPolicy: func(name string) (pbl.FilterPolicy, error) {
			switch name {
			case "none":
				return nil, nil
			case "rocksdb.BuiltinBloomFilter":
				return bloom.FilterPolicy(10), nil
			default:
				return nil, fmt.Errorf("unknown filter policy: %s", name)
			}
		},
		SkipUnknown: func(name, value string) bool {
			log.Errorw("Unknown pebble option", "name", name, "value", value)
			return true
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cannot parse the pebble options: %w", err)
	}
	return opts, nil
}
