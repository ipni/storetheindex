package dsstream

import (
	"bytes"
	"context"
	"fmt"
	"iter"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/storeutil"
	"github.com/ipni/storetheindex/internal/dskey"
)

// NewTarget returns the import target that writes the kinds of NewSource
// into an indexer's datastore. Write adds each kind's prefix back to the
// wire keys and puts the entries in datastore batches of about
// storeutil.CommitBytes, so that writing a part again leaves the same keys.
// Sync syncs the datastore.
//
// An entry that breaks its kind's wire encoding is a permanent error, and so
// is a failed write:
//   - a key is not empty and has no '/';
//   - a value is not empty;
//   - a marker's status is processed (1) or skipped (3).
func NewTarget(ds datastore.Batching) importer.Target {
	return target{ds: ds}
}

type target struct {
	ds datastore.Batching
}

// Write implements importer.Target
func (t target) Write(ctx context.Context, kind importer.Kind, entries iter.Seq2[importer.Entry, error]) error {
	prefix, err := kindPrefix(kind)
	if err != nil {
		return err
	}
	b, err := t.ds.Batch(ctx)
	if err != nil {
		return importer.PermanentError(err)
	}

	// pending counts the bytes put in b since it was last committed
	pending := 0
	for e, err := range entries {
		if err != nil {
			return err
		}
		if err := checkImportEntry(kind, e); err != nil {
			return importer.PermanentError(err)
		}

		// A batch may keep the value, and the entry's slices are reused
		if err := b.Put(ctx, datastore.RawKey(prefix+string(e.Key)), bytes.Clone(e.Value)); err != nil {
			return importer.PermanentError(err)
		}
		if pending += len(prefix) + len(e.Key) + len(e.Value); pending >= storeutil.CommitBytes {
			if err := commitBatch(ctx, b); err != nil {
				return err
			}
			if b, err = t.ds.Batch(ctx); err != nil {
				return importer.PermanentError(err)
			}
			pending = 0
		}
	}
	if pending == 0 {
		return nil
	}
	return commitBatch(ctx, b)
}

// commitBatch commits b, unless ctx is done
func commitBatch(ctx context.Context, b datastore.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.Commit(ctx); err != nil {
		return importer.PermanentError(err)
	}
	return nil
}

// checkImportEntry returns an error when e breaks the wire encoding of kind
func checkImportEntry(kind importer.Kind, e importer.Entry) error {
	switch {
	case len(e.Key) == 0 || bytes.IndexByte(e.Key, '/') >= 0:
		return fmt.Errorf("%w: key %q is empty or has a '/'", importer.ErrWireEncoding, e.Key)
	case len(e.Value) == 0:
		return fmt.Errorf("%w: key %q has an empty value", importer.ErrWireEncoding, e.Key)
	case kind == importer.KindAdProcessed && e.Value[0] != dskey.AdMarkerProcessed && e.Value[0] != dskey.AdMarkerSkipped:
		return fmt.Errorf("%w: marker %q has status %d", importer.ErrWireEncoding, e.Key, e.Value[0])
	}
	return nil
}

// Sync implements importer.Target
func (t target) Sync() error {
	return t.ds.Sync(context.Background(), datastore.NewKey("/"))
}
