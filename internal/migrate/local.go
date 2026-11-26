package migrate

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/storetheindex/internal/dskey"
)

var log = logging.Logger("indexer/migrate")

// recheckSync drops the copied /sync entries whose ad the source has marked
// for resync since they were copied.
//
// The source sends no /sync entry whose ad is marked for resync, since the
// destination's sync would stop at that ad and never process it (see
// dsstream). The first pass, the Sync step, leaves out the publishers that
// are in a resync when it reads them. This second pass, after the provider
// records, reads the source's /sync entries again and drops each copied
// entry whose publisher the source no longer sends: those whose resync
// started after the first pass. Together, the destination keeps a /sync
// entry only for a publisher that was in a resync at neither pass.
//
// Dropping an entry is always safe: without it, the destination walks the
// publisher's whole chain and processes every ad that has no processed or
// skipped marker. A publisher whose entry moved on to a newer ad keeps its
// copied entry: its newer ads have no markers on the destination, which
// processes them.
//
// A resync that starts after the first pass and finishes before this one
// keeps its entry, since the source sends it again. That is correct: a
// resync processes the same ads again, so the copied markers and entry
// still name processed ads, and what it changed is in the source's value
// store, which is copied after this step. A resync that runs while the
// value store is copied is not covered; the source must not be resynced
// during an import (see doc/migration.md).
//
// The step reads every /sync entry of the source again, one per publisher,
// and keeps their keys in memory. It writes no progress: run again, it
// starts over, and a drop that is done stays done.
func (r *run) recheckSync(ctx context.Context, src importer.Source) error {
	if r.state.step(stepSyncRecheck).Done {
		return nil
	}
	current := &keySet{keys: make(map[string]struct{})}
	if err := importer.NewKindManager(importer.KindSync, src, current, r.opts...).Run(ctx); err != nil {
		return fmt.Errorf("cannot read the source's /sync entries again: %w", err)
	}

	results, err := r.ds.Query(ctx, query.Query{Prefix: dskey.SyncPrefix, KeysOnly: true})
	if err != nil {
		return fmt.Errorf("cannot list the copied /sync entries: %w", err)
	}
	defer func() { _ = results.Close() }()
	b, err := r.ds.Batch(ctx)
	if err != nil {
		return err
	}
	var dropped int
	for res := range results.Next() {
		if res.Error != nil {
			return fmt.Errorf("cannot list the copied /sync entries: %w", res.Error)
		}
		publisher := strings.TrimPrefix(res.Key, dskey.SyncPrefix)
		if _, ok := current.keys[publisher]; ok {
			continue
		}
		if err := b.Delete(ctx, datastore.NewKey(res.Key)); err != nil {
			return err
		}
		dropped++
	}
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("cannot drop the /sync entries: %w", err)
	}
	log.Infow("Rechecked the copied /sync entries", "dropped", dropped)

	r.state.step(stepSyncRecheck).Done = true
	return r.save(ctx)
}

// recordValidator is a value store that deletes the records that its
// context map does not lead to, in batches from a position, as the pebble
// store's ValidateRecords does
type recordValidator interface {
	ValidateRecords(ctx context.Context, from []byte, limit int) (next []byte, done bool, deleted int, err error)
}

// validateBatch is how many records validateRecords checks between two
// stores of its progress
const validateBatch = 10_000

// validateRecords deletes the copied records that the copied context map
// does not lead to, so that each context has at most one record, which the
// destination's removal of a context relies on. The records and the
// context map are copied at different times, so a context that the source
// removed or replaced in between leaves records that no context leads to.
// The destination ingests those changes again, which corrects what the
// check leaves.
//
// It checks the records in batches and stores its position after each, so
// a run that stops resumes there. It reads every record, which can take
// hours on a large store. Without validate, or with a value store that
// cannot validate its records, the step is done with a warning, and those
// records stay until a later cleanup; a check stopped part way is not
// resumed.
func (r *run) validateRecords(ctx context.Context, valueStore importer.Target, validate bool) error {
	step := r.state.step(stepValidate)
	if step.Done {
		return nil
	}
	v, ok := valueStore.(recordValidator)
	if !validate || !ok {
		reason := "the check is turned off"
		if !ok {
			reason = "the value store cannot validate its records"
		}
		log.Warnw("Skipping the validation of the copied records; the records of contexts that changed during the import stay", "reason", reason)
		step.Done, step.Progress = true, nil
		return r.save(ctx)
	}
	var deleted int
	for {
		next, done, n, err := v.ValidateRecords(ctx, step.Progress, validateBatch)
		if err != nil {
			return fmt.Errorf("cannot validate the copied records: %w", err)
		}
		deleted += n
		if done {
			log.Infow("Validated the copied records", "deleted", deleted)
			step.Done, step.Progress = true, nil
			return r.save(ctx)
		}
		step.Progress = next
		if err := r.save(ctx); err != nil {
			return err
		}
	}
}

// keySet is an importer.Target that keeps the keys it is given
type keySet struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func (k *keySet) Write(_ context.Context, _ importer.Kind, entries iter.Seq2[importer.Entry, error]) error {
	for e, err := range entries {
		if err != nil {
			return err
		}
		k.mu.Lock()
		k.keys[string(e.Key)] = struct{}{}
		k.mu.Unlock()
	}
	return nil
}

func (k *keySet) Sync() error { return nil }
