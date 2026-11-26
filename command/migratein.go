package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	pbl "github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/storetheindex/internal/metrics"
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
		&cli.StringFlag{
			Name:  "status-listen",
			Usage: "host:port of the import's status port: GET /migrate/v1/status, PUT /migrate/v1/workers {\"n\": 16}, GET /metrics. Empty turns it off.",
			Value: "127.0.0.1:3010",
		},
		&cli.DurationFlag{
			Name:  "console-interval",
			Usage: "Time between two lines of the import's progress in the log. 0 turns them off.",
			Value: 30 * time.Second,
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

	im := migrate.New(ds, src, vs.(importer.Target), cctx.Bool("validate-records"),
		importer.WithWorkers(cctx.Int("workers")),
		importer.WithStagger(cctx.Duration("worker-stagger")),
		importer.WithRetryWindow(cctx.Duration("retry-window")))
	if addr := cctx.String("status-listen"); addr != "" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("cannot listen for the import's status: %w", err)
		}
		srv := &http.Server{Handler: newMigrateStatusHandler(im), ReadHeaderTimeout: time.Minute}
		go func() { _ = srv.Serve(l) }()
		defer func() { _ = srv.Close() }()
		log.Infow("Import status listening", "addr", l.Addr())
	}
	if interval := cctx.Duration("console-interval"); interval > 0 {
		ctx, cancel := context.WithCancel(cctx.Context)
		defer cancel()
		go logMigrateStatus(ctx, im, interval)
	}

	log.Infow("Importing", "source", cctx.String("source-url"))
	err = im.Run(cctx.Context)
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

// migrateStatusPath is the path prefix of an import's status and control on
// its status port
const migrateStatusPath = "/migrate/v1/"

// newMigrateStatusHandler returns the handler of an import's status port:
// GET /migrate/v1/status returns im's status as JSON, PUT
// /migrate/v1/workers with {"n": n} sets its number of workers, and
// /metrics serves the import's metrics
func newMigrateStatusHandler(im *migrate.Import) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+migrateStatusPath+"status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(im.Status())
	})
	mux.HandleFunc("PUT "+migrateStatusPath+"workers", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			N *int `json:"n"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&req); err != nil || req.N == nil {
			http.Error(w, `the body must be {"n": <number of workers>}`, http.StatusBadRequest)
			return
		}
		if err := im.SetWorkers(*req.N); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/metrics", migrateMetricsHandler())
	return mux
}

// migrateMetricsHandler serves the import's metrics. It is made once per
// process: each metrics.Start adds an exporter of the same metrics to the
// default Prometheus registry, and two would fail every scrape.
var migrateMetricsHandler = sync.OnceValue(func() http.Handler {
	return metrics.Start(slices.Concat(coremetrics.ImportViews, migrate.Views))
})

// logMigrateStatus logs a line of im's progress every interval until ctx
// ends
func logMigrateStatus(ctx context.Context, im *migrate.Import, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		st := im.Status()
		var step string
		for _, s := range st.Steps {
			if s.State == "running" {
				step = s.Name
			}
		}
		if st.Running == nil {
			log.Infow("Import progress", "step", step)
			continue
		}
		r := st.Running
		log.Infow("Import progress", "step", step, "entries", r.Entries, "bytes", r.Bytes,
			"partsWritten", r.PartsWritten, "partsInFlight", r.PartsInFlight, "retries", r.Retries,
			"workers", r.Workers, "workersRunning", r.WorkersRunning)
	}
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
