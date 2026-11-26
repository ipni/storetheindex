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
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/dsstream"
)

// The names of an import's steps: one per kind, and the local steps
const (
	stepAdProcessed  = "adProcessed"
	stepAdSkipReason = "adSkipReason"
	stepSync         = "sync"
	stepProviders    = "providers"
	// stepSyncRecheck drops the copied /sync entries whose ad the source
	// has since marked for resync (see run.recheckSync)
	stepSyncRecheck = "syncRecheck"
	stepRecords     = "records"
	stepContextMap  = "contextMap"
	// stepValidate deletes the copied records that the copied context map
	// does not lead to (see run.validateRecords)
	stepValidate    = "validate"
	stepMultihashes = "multihashes"
)

// stepNames are the names of an import's steps, in the order they run
var stepNames = []string{
	stepAdProcessed,
	stepAdSkipReason,
	stepSync,
	stepProviders,
	stepSyncRecheck,
	stepRecords,
	stepContextMap,
	stepValidate,
	stepMultihashes,
}

// State is the state of an import, stored under dskey.MigrateState
type State struct {
	// Started is when the import started, zero before it starts
	Started time.Time `json:"started,omitzero"`
	// Steps are the import's steps, in the order they run (see stepNames)
	Steps []Step `json:"steps"`
}

// Step is the state of one step of an import
type Step struct {
	Name string `json:"name"`
	Done bool   `json:"done,omitempty"`
	// Progress is where the step resumes: the start of the kind's lowest
	// part not yet written, nil for its beginning
	Progress []byte `json:"progress,omitempty"`
}

// newState returns the state of an import that has not started
func newState() State {
	var s State
	for _, name := range stepNames {
		s.Steps = append(s.Steps, Step{Name: name})
	}
	return s
}

// step returns the step named name of a state that LoadState or newState
// returned
func (s *State) step(name string) *Step {
	return &s.Steps[slices.Index(stepNames, name)]
}

// Done reports whether every step of the import is done
func (s State) Done() bool {
	for _, step := range s.Steps {
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

// LoadState returns the import's state in ds, or the state of an import
// that has not started when there is none. A stored state whose steps are
// not this version's steps, in its order, is an error.
func LoadState(ctx context.Context, ds datastore.Datastore) (State, error) {
	var s State
	data, err := ds.Get(ctx, datastore.NewKey(dskey.MigrateState))
	if errors.Is(err, datastore.ErrNotFound) {
		return newState(), nil
	}
	if err != nil {
		return s, fmt.Errorf("cannot read the import state: %w", err)
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("cannot decode the import state: %w", err)
	}
	names := make([]string, len(s.Steps))
	for i, step := range s.Steps {
		names[i] = step.Name
	}
	if !slices.Equal(names, stepNames) {
		return s, fmt.Errorf("the import state has the steps %v, not %v", names, stepNames)
	}
	return s, nil
}

// Import is an import into this indexer, with its status and control while it runs
type Import struct {
	ds         datastore.Batching
	src        importer.Source
	valueStore importer.Target
	validate   bool
	opts       []importer.ManagerOption

	mu sync.Mutex
	// workers is the number of workers of each kind, once SetWorkers has
	// set it; until then the options set it
	workers int
	// running is the manager of the running kind, nil when none runs
	running *importer.KindManager
	// saved is the state as last stored, nil before Run loads it
	saved *State
}

// New returns an import of every kind of src: the datastore kinds into ds,
// and the value store kinds into valueStore. opts apply to the manager of
// every kind. Without validate, the records that the copied context map
// does not lead to are not deleted (see run.validateRecords).
//
// ds must sync its writes, as go-ds-leveldb does by default: the state is
// stored with a Put, after the target of the kind has synced the parts
// before its progress.
func New(ds datastore.Batching, src importer.Source, valueStore importer.Target, validate bool, opts ...importer.ManagerOption) *Import {
	return &Import{ds: ds, src: src, valueStore: valueStore, validate: validate, opts: opts, workers: -1}
}

// SetWorkers sets the number of workers of the running kind and of the
// kinds after it (see importer.KindManager.SetWorkers)
func (im *Import) SetWorkers(n int) error {
	if n < 0 || n > importer.MaxWorkers {
		return fmt.Errorf("the number of workers must be from 0 to %d, not %d", importer.MaxWorkers, n)
	}
	im.mu.Lock()
	defer im.mu.Unlock()
	im.workers = n
	recordWorkers(n)
	if im.running != nil {
		return im.running.SetWorkers(n)
	}
	return nil
}

// Status is the state of an import for its operator
type Status struct {
	Started time.Time    `json:"started,omitzero"`
	Steps   []StepStatus `json:"steps"`
	// Running is the work of the kind that runs, absent when none does
	Running *importer.KindStats `json:"running,omitempty"`
}

// StepStatus is the state of one step: pending, running or done
type StepStatus struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// Status returns the import's status: its steps as last stored, and the
// work of the kind that runs
func (im *Import) Status() Status {
	im.mu.Lock()
	defer im.mu.Unlock()
	st := newState()
	if im.saved != nil {
		st = *im.saved
	}
	status := Status{Started: st.Started}
	for i, step := range st.Steps {
		status.Steps = append(status.Steps, StepStatus{Name: step.Name, State: stepState(st, i)})
	}
	if im.running != nil {
		stats := im.running.Stats()
		status.Running = &stats
	}
	return status
}

// stepState returns the state of the step at index i of st: done, running
// for the first step that is not done once the import has started, or
// pending
func stepState(st State, i int) string {
	switch {
	case st.Steps[i].Done:
		return "done"
	case !st.Started.IsZero() && !slices.ContainsFunc(st.Steps[:i], func(s Step) bool { return !s.Done }):
		return "running"
	default:
		return "pending"
	}
}

// Run runs the import and returns nil once it is done. It resumes from the
// state in ds: a step that is done is skipped, and the step that was running
// resumes from its progress.
func (im *Import) Run(ctx context.Context) error {
	ds, src, valueStore, validate := im.ds, im.src, im.valueStore, im.validate
	st, err := LoadState(ctx, ds)
	if err != nil {
		return err
	}
	r := &run{ds: ds, state: st, opts: im.opts, im: im}
	im.setSaved(st)
	if r.state.Started.IsZero() {
		r.state.Started = time.Now()
		if err := r.save(ctx); err != nil {
			return err
		}
	}

	dsTarget := dsstream.NewTarget(ds)
	if err := r.copyKind(ctx, stepAdProcessed, importer.KindAdProcessed, src, dsTarget); err != nil {
		return err
	}
	if err := r.copyKind(ctx, stepAdSkipReason, importer.KindAdSkipReason, src, dsTarget); err != nil {
		return err
	}
	if err := r.copyKind(ctx, stepSync, importer.KindSync, src, dsTarget); err != nil {
		return err
	}
	if err := r.copyKind(ctx, stepProviders, importer.KindProviders, src, dsTarget); err != nil {
		return err
	}
	if err := r.recheckSync(ctx, src); err != nil {
		return err
	}
	if err := r.copyKind(ctx, stepRecords, importer.KindRecords, src, valueStore); err != nil {
		return err
	}
	if err := r.copyKind(ctx, stepContextMap, importer.KindContextMap, src, valueStore); err != nil {
		return err
	}
	if err := r.validateRecords(ctx, valueStore, validate); err != nil {
		return err
	}
	if err := r.copyKind(ctx, stepMultihashes, importer.KindMultihashes, src, valueStore); err != nil {
		return err
	}

	return nil
}

// run is one run of an import. Its kinds run one after another, and a
// kind's progress reports are serialized and end before its Run returns, so
// nothing changes the state concurrently. Status reads the copy that each
// save gives im.
type run struct {
	ds    datastore.Batching
	opts  []importer.ManagerOption
	state State
	// im, when set, is the Import that the run reports to
	im *Import
}

// copyKind copies kind from src to dst, from the progress of the step named
// name, and marks the step done. A step that is done is not copied again.
func (r *run) copyKind(ctx context.Context, name string, kind importer.Kind, src importer.Source, dst importer.Target) error {
	step := r.state.step(name)
	if step.Done {
		return nil
	}

	opts := r.opts
	if r.im != nil {
		if n := r.im.workerCount(); n >= 0 {
			opts = slices.Concat(opts, []importer.ManagerOption{importer.WithWorkers(n)})
		}
	}
	m := importer.NewKindManager(kind, src, dst, slices.Concat(opts, []importer.ManagerOption{
		importer.WithStart(step.Progress),
		importer.WithProgress(func(progress []byte) error {
			if err := dst.Sync(); err != nil {
				return fmt.Errorf("cannot sync the %s target: %w", kind, err)
			}
			step.Progress = progress
			return r.save(ctx)
		}),
	})...)
	if r.im != nil {
		r.im.setRunning(m)
		defer r.im.setRunning(nil)
	}
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
	if r.im != nil {
		r.im.setSaved(r.state)
	}
	return nil
}

// workerCount returns the number of workers that SetWorkers set, or -1
func (im *Import) workerCount() int {
	im.mu.Lock()
	defer im.mu.Unlock()
	return im.workers
}

// setRunning sets the manager of the running kind
func (im *Import) setRunning(m *importer.KindManager) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.running = m
	if m != nil {
		recordWorkers(m.Stats().Workers)
	}
}

// setSaved keeps a copy of the stored state st for Status, apart from the
// state that the run goes on changing, and records its steps' states in the
// metrics
func (im *Import) setSaved(st State) {
	im.mu.Lock()
	defer im.mu.Unlock()
	st.Steps = slices.Clone(st.Steps)
	im.saved = &st
	recordSteps(st)
}
