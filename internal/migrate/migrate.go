// Package migrate imports another indexer's data into this one: its
// datastore's kinds and its value store's kinds, one kind after another,
// each copied in parallel parts (see importer.KindManager), with local
// steps between and after them. The import's state is kept in the
// destination's datastore, so that a run that stops resumes where it
// stopped.
package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/dsstream"
)

// State is the state of an import, stored under dskey.MigrateState. It has
// one step per kind and per local step, which run in the order of its
// fields. A zero State is an import that has not started.
type State struct {
	Started      time.Time `json:"started,omitzero"`
	AdProcessed  Step      `json:"adProcessed"`
	AdSkipReason Step      `json:"adSkipReason"`
	Sync         Step      `json:"sync"`
	Providers    Step      `json:"providers"`
	// SyncRecheck drops the copied /sync entries whose ad the source has
	// since marked for resync (see run.recheckSync)
	SyncRecheck Step `json:"syncRecheck"`
	Records     Step `json:"records"`
	ContextMap  Step `json:"contextMap"`
	// Validate deletes the copied records that the copied context map does
	// not lead to (see run.validateRecords)
	Validate    Step `json:"validate"`
	Multihashes Step `json:"multihashes"`
}

// Step is the state of one step of an import
type Step struct {
	Done bool `json:"done,omitempty"`
	// Progress is where the step resumes: the start of the kind's lowest
	// part not yet written, nil for its beginning
	Progress []byte `json:"progress,omitempty"`
}

// Done reports whether every step of the import is done
func (s State) Done() bool {
	for _, step := range []Step{
		s.AdProcessed,
		s.AdSkipReason,
		s.Sync,
		s.Providers,
		s.SyncRecheck,
		s.Records,
		s.ContextMap,
		s.Validate,
		s.Multihashes,
	} {
		if !step.Done {
			return false
		}
	}
	return true
}

// ErrUnfinished is the error of CheckFinished when an import has started
// and is not done
var ErrUnfinished = errors.New("an import into this indexer started and is not done; run migrate-in again to finish it")

// CheckFinished returns ErrUnfinished when an import into ds has started
// and is not done, and nil when it is done or none has started. The
// indexer must not run on stores that hold part of an import.
func CheckFinished(ctx context.Context, ds datastore.Datastore) error {
	st, err := LoadState(ctx, ds)
	if err != nil {
		return err
	}
	if !st.Started.IsZero() && !st.Done() {
		return ErrUnfinished
	}
	return nil
}

// LoadState returns the import's state in ds, or a zero State when the
// import has not started
func LoadState(ctx context.Context, ds datastore.Datastore) (State, error) {
	var s State
	data, err := ds.Get(ctx, datastore.NewKey(dskey.MigrateState))
	if errors.Is(err, datastore.ErrNotFound) {
		return s, nil
	}
	if err != nil {
		return s, fmt.Errorf("cannot read the import state: %w", err)
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("cannot decode the import state: %w", err)
	}
	return s, nil
}

// Run imports every kind of src: the datastore kinds into ds, and the value
// store kinds into valueStore, and returns nil once the import is done. It
// resumes from the state in ds: a step that is done is skipped, and the
// step that was running resumes from its progress. opts apply to the
// manager of every kind. Without validate, the records that the copied
// context map does not lead to are not deleted (see run.validateRecords).
//
// ds must sync its writes, as go-ds-leveldb does by default: the state is
// stored with a Put, after the target of the kind has synced the parts
// before its progress.
func Run(ctx context.Context, ds datastore.Batching, src importer.Source, valueStore importer.Target, validate bool, opts ...importer.ManagerOption) error {
	st, err := LoadState(ctx, ds)
	if err != nil {
		return err
	}
	r := &run{ds: ds, state: st, opts: opts}
	if r.state.Started.IsZero() {
		r.state.Started = time.Now()
		if err := r.save(ctx); err != nil {
			return err
		}
	}

	dsTarget := dsstream.NewTarget(ds)
	for _, k := range []struct {
		step *Step
		kind importer.Kind
	}{
		{&r.state.AdProcessed, importer.KindAdProcessed},
		{&r.state.AdSkipReason, importer.KindAdSkipReason},
		{&r.state.Sync, importer.KindSync},
		{&r.state.Providers, importer.KindProviders},
	} {
		if err := r.copyKind(ctx, k.step, k.kind, src, dsTarget); err != nil {
			return err
		}
	}
	if err := r.recheckSync(ctx, src); err != nil {
		return err
	}
	for _, k := range []struct {
		step *Step
		kind importer.Kind
	}{
		{&r.state.Records, importer.KindRecords},
		{&r.state.ContextMap, importer.KindContextMap},
	} {
		if err := r.copyKind(ctx, k.step, k.kind, src, valueStore); err != nil {
			return err
		}
	}
	if err := r.validateRecords(ctx, valueStore, validate); err != nil {
		return err
	}
	return r.copyKind(ctx, &r.state.Multihashes, importer.KindMultihashes, src, valueStore)
}

// run is one run of an import. Its kinds run one after another, and a
// kind's progress reports are serialized and end before its Run returns, so
// nothing changes the state concurrently.
type run struct {
	ds    datastore.Batching
	opts  []importer.ManagerOption
	state State
}

// copyKind copies kind from src to dst, from the progress of step, and
// marks the step done. A step that is done is not copied again.
func (r *run) copyKind(ctx context.Context, step *Step, kind importer.Kind, src importer.Source, dst importer.Target) error {
	if step.Done {
		return nil
	}

	m := importer.NewKindManager(kind, src, dst, slices.Concat(r.opts, []importer.ManagerOption{
		importer.WithStart(step.Progress),
		importer.WithProgress(func(progress []byte) error {
			if err := dst.Sync(); err != nil {
				return fmt.Errorf("cannot sync the %s target: %w", kind, err)
			}
			step.Progress = progress
			return r.save(ctx)
		}),
	})...)
	if err := m.Run(ctx); err != nil {
		return fmt.Errorf("cannot import %s: %w", kind, err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("cannot sync the %s target: %w", kind, err)
	}
	step.Done, step.Progress = true, nil
	return r.save(ctx)
}

// save stores the state
func (r *run) save(ctx context.Context) error {
	data, err := json.Marshal(r.state)
	if err != nil {
		return err
	}
	if err := r.ds.Put(ctx, datastore.NewKey(dskey.MigrateState), data); err != nil {
		return fmt.Errorf("cannot store the import state: %w", err)
	}
	return nil
}
