package filestore

import (
	"context"
	"errors"
	"io"
	"io/fs"
)

var _ Interface = (*Replicator)(nil)

// Replicator opportunistically replicates data from Replicator.Source onto Replicator.Destination and
// uses Replicator.Destination whenever possible, falling back on Replicator.Source upon fs.ErrNotExist errors.
type Replicator struct {
	Destination Interface
	Source      Interface
}

func (r *Replicator) Delete(ctx context.Context, path string) error {
	return r.Destination.Delete(ctx, path)
}

// Get Attempts to get from Replicator.Destination first, and if path does nto exist falls back onto Replicator.Source.
// If the path is found at source, it is replicated onto destination first and then the replica is returned.
func (r *Replicator) Get(ctx context.Context, path string) (*File, io.ReadCloser, error) {
	dFile, dRc, dErr := r.Destination.Get(ctx, path)
	if dErr != nil {
		if errors.Is(dErr, fs.ErrNotExist) {
			_, sRc, sErr := r.Source.Get(ctx, path)
			if sErr != nil {
				// Cannot get from source; whatever the error, simply return it since it makes no difference.
				return nil, nil, dErr

			}

			// TODO Consider doing streaming put via Tee once read semantics is understood.
			//      This would reduce IOPS by coping data while it is being read.
			//      However, for this to work properly, the "Get"er should read fully and quickly.
			//      Otherwise, we end up with a lot of half written garbage during streaming Put.
			//      Also concurrency is a concern since in local storage files are written directly
			//      at path, which can interfere with Get on the same path while data is being replicated.
			//      For now, simply Put first then Get to avoid the complexities listed above.

			// Replicate the file into destination
			if _, err := r.Destination.Put(ctx, path, sRc); err != nil {
				return nil, nil, err
			}
			// Get it from destination
			return r.Destination.Get(ctx, path)
		}
	}
	return dFile, dRc, nil
}

func (r *Replicator) Head(ctx context.Context, path string) (*File, error) {
	dFile, dErr := r.Destination.Head(ctx, path)
	if dErr != nil {
		return r.Source.Head(ctx, path)
	}
	return dFile, nil
}

func (r *Replicator) List(ctx context.Context, path string, recursive bool) (<-chan *File, <-chan error) {
	c := make(chan *File, 1)
	e := make(chan error, 1)

	go func() {
		defer close(e)
		defer close(c)

		// Attempt listing from Destination first.
		// If Destination returns a least one *File prior to returning any errors, then
		// continue listing from destination may it return further *File or error.
		//
		// If Destination returns an error first, then fallback on listing from Source.

		{
			dC, dE := r.Destination.List(ctx, path, recursive)
			var listedAtLeastOneFile bool
		DestinationList:
			for {
				select {
				case <-ctx.Done():
					return
				case dee, ok := <-dE:
					if listedAtLeastOneFile {
						if ok {
							e <- dee
						}
						return
					} else {
						break DestinationList
					}
				case dcc, ok := <-dC:
					if ok {
						c <- dcc
						listedAtLeastOneFile = true
					} else {
						if listedAtLeastOneFile {
							return
						} else {
							break DestinationList
						}
					}
				}
			}
		}

		{
			sC, sE := r.Source.List(ctx, path, recursive)
			for {
				select {
				case <-ctx.Done():
					return
				case see := <-sE:
					e <- see
					return
				case dcc := <-sC:
					c <- dcc
				}
			}
		}
	}()
	return c, e
}

func (r *Replicator) Put(ctx context.Context, path string, reader io.Reader) (*File, error) {
	return r.Destination.Put(ctx, path, reader)
}

func (r *Replicator) Type() string {
	return "replicator"
}
