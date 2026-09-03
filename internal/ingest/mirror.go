package ingest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/libp2p/go-libp2p/core/peer"
)

type adMirror struct {
	mainCarReader      *carstore.CarReader
	mainCarWriter      *carstore.CarWriter
	externalCarReaders []*carstore.CarReader
	exposableFilestore filestore.Interface
}

func (m adMirror) canRead() bool {
	return m.mainCarReader != nil || len(m.externalCarReaders) > 0
}
func (m adMirror) canWrite() bool {
	return m.mainCarWriter != nil
}

func (m adMirror) cleanupAdData(ctx context.Context, adCid cid.Cid, skipEntries bool) error {
	return m.mainCarWriter.CleanupAdData(ctx, adCid, skipEntries)
}

type adDataSource int

const (
	adDataSourceNone     adDataSource = iota // no data source
	adDataSourceMain                         // data read from the main storage
	adDataSourceExternal                     // data read from an external reader storage
	adDataSourceProvider                     // data read from the provider
)

func (d adDataSource) String() string {
	switch d {
	case adDataSourceNone:
		return "none"
	case adDataSourceMain:
		return "main"
	case adDataSourceExternal:
		return "external"
	case adDataSourceProvider:
		return "provider"
	default:
		return fmt.Sprintf("unknown(%d)", d)
	}
}

func (d adDataSource) canBeWritten() bool {
	// Main is already on the write store; do not copy it back for rewriting.
	switch d {
	case adDataSourceExternal, adDataSourceProvider:
		return true
	default:
		return false
	}
}

// readMain reads a CAR from Main, or reports fs.ErrNotExist when Main is not
// readable. A successful Read does not mean the entries chain is usable; ingest
// validates that separately and falls back to External when it is not.
func (m adMirror) readMain(
	ctx context.Context,
	adCid cid.Cid,
	skipEntries bool,
) (
	adBlock *carstore.AdBlock,
	location string,
	err error,
) {
	if m.mainCarReader == nil {
		return nil, "", fs.ErrNotExist
	}
	adBlock, err = m.mainCarReader.Read(ctx, adCid, skipEntries)
	if err != nil {
		return nil, "", err
	}
	return adBlock, m.mainCarReader.Location(), nil
}

// readExternalRace races all External readers. The first successful Read wins;
// others are cancelled. 404s and other errors are misses. If every peer misses,
// or no External readers are configured, returns fs.ErrNotExist.
func (m adMirror) readExternalRace(
	ctx context.Context,
	adCid cid.Cid,
	skipEntries bool,
) (
	block *carstore.AdBlock,
	source adDataSource,
	location string,
	err error,
) {
	if len(m.externalCarReaders) == 0 {
		return nil, adDataSourceNone, "", fs.ErrNotExist
	}

	type externalReadResult struct {
		idx   int
		block *carstore.AdBlock
		err   error
	}

	var wg sync.WaitGroup

	n := len(m.externalCarReaders)
	results := make(chan externalReadResult, n)
	cancels := make([]context.CancelFunc, n)

	for i, reader := range m.externalCarReaders {
		rctx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel

		wg.Go(func() {
			block, readErr := reader.Read(rctx, adCid, skipEntries)
			results <- externalReadResult{idx: i, block: block, err: readErr}
		})
	}

	checkRes := func(res externalReadResult) bool {
		switch {
		case res.err == nil:
			return true

		case errors.Is(res.err, fs.ErrNotExist), errors.Is(res.err, context.Canceled):
			log.Debugw("External CAR mirror race lost", "index", res.idx, "adCid", adCid)
			return false

		default:
			log.Warnw("Cannot read advertisement from external filestore", "err", res.err, "index", res.idx, "carPath", adCid)
			return false
		}
	}

	defer func() {
		// Cancel all readers that did not succeed.
		// Note: to avoid cancelling the context when the race is won,
		// we overwrite the cancel function after the race is won.
		for _, cancel := range cancels {
			cancel()
		}

		// Wait for the race to complete.
		wg.Wait()

		// All readers must have already put their results on the channel, so we can close it.
		close(results)

		// Drain entry streams from losing successes. Cancels above must run
		// first: drain alone would otherwise read entire CARs, and without a
		// receiver the cancelled readEntries goroutines block forever on their
		// final error send.
		for res := range results {
			if checkRes(res) {
				res.block.Close()
			}
		}
	}()

	for range n {
		select {
		case <-ctx.Done():
			return nil, adDataSourceNone, "", ctx.Err()

		case res := <-results:
			if checkRes(res) {
				// Overwrite the cancel function for the winner so it is not called when the context is cancelled.
				cancels[res.idx] = func() {}
				log.Debugw(
					"External CAR mirror race won",
					"index", res.idx,
					"adCid", adCid,
				)
				return res.block, adDataSourceExternal, m.externalCarReaders[res.idx].Location(), nil
			}
		}
	}

	return nil, adDataSourceNone, "", fs.ErrNotExist
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, noOverwrite bool) (*filestore.File, error) {
	return m.mainCarWriter.Write(ctx, adCid, skipEntries, noOverwrite)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.mainCarWriter.WriteHead(ctx, adCid, publisher)
}

func newMirror(cfgMirror config.Mirror, dstore datastore.Batching) (m adMirror, err error) {
	if !cfgMirror.MainMode.Valid() {
		return m, fmt.Errorf("invalid AdvertisementMirror.MainMode %q", cfgMirror.MainMode)
	}

	// MainMode controls a single Main filestore shared by reader and writer.
	if cfgMirror.MainMode.Enabled() {
		switch mainStore, err := filestore.MakeFilestore(cfgMirror.Main.Config); {
		case err != nil:
			return m, fmt.Errorf("cannot create main car file store for mirror: %w", err)

		case mainStore == nil:
			log.Warnw("Main mirror is enabled with no storage backend", "backendType", cfgMirror.Main.Type)

		default:
			if cfgMirror.MainMode.CanWrite() {
				m.mainCarWriter, err = carstore.NewWriter(dstore, mainStore, carstore.WithCompress(cfgMirror.Main.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file writer: %w", err)
				}
			}

			if cfgMirror.MainMode.CanRead() {
				m.mainCarReader, err = carstore.NewReader(mainStore, carstore.WithCompress(cfgMirror.Main.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file reader: %w", err)
				}
				m.exposableFilestore = mainStore
			}
		}
	}

	// External is independent of MainMode: when configured, all entries are
	// raced in parallel after a Main miss or unusable Main CAR (sole sources if
	// Main read is off).
	for i, ext := range cfgMirror.External {
		if ext.Type == "" || ext.Type == "none" {
			continue
		}
		if cfgMirror.MainMode.Enabled() && reflect.DeepEqual(ext, cfgMirror.Main) {
			return m, fmt.Errorf("external[%d] retrieval cannot be the same as the main backend", i)
		}

		externalReadStore, err := filestore.MakeFilestore(ext.Config)
		if err != nil {
			return m, fmt.Errorf("cannot create external[%d] car file retrieval for mirror: %w", i, err)
		}
		if externalReadStore == nil {
			continue
		}

		reader, err := carstore.NewReader(externalReadStore, carstore.WithCompress(ext.Compress))
		if err != nil {
			return m, fmt.Errorf("cannot create mirror car file external[%d] reader: %w", i, err)
		}
		m.externalCarReaders = append(m.externalCarReaders, reader)
	}

	return m, nil
}
