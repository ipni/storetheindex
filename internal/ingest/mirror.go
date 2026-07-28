package ingest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/libp2p/go-libp2p/core/peer"
)

type adMirror struct {
	carReader          *carstore.CarReader
	carExternalReader  *carstore.CarReader
	carWriter          *carstore.CarWriter
	rdWrSame           bool
	exposableFilestore filestore.Interface
}

func (m adMirror) canRead() bool {
	return m.carReader != nil || m.carExternalReader != nil
}
func (m adMirror) canWrite() bool {
	return m.carWriter != nil
}

func (m adMirror) cleanupAdData(ctx context.Context, adCid cid.Cid, skipEntries bool) error {
	return m.carWriter.CleanupAdData(ctx, adCid, skipEntries)
}

type adDataSource int

const (
	adDataSourceNone     adDataSource = iota // no data source
	adDataSourceLocal                        // data read from the local storage
	adDataSourceExternal                     // data read from the external reader storage
	adDataSourceProvider                     // data read from the provider
)

func (d adDataSource) String() string {
	switch d {
	case adDataSourceNone:
		return "none"
	case adDataSourceLocal:
		return "local"
	case adDataSourceExternal:
		return "exernal"
	case adDataSourceProvider:
		return "provider"
	default:
		return fmt.Sprintf("unknown(%d)", d)
	}
}

func (d adDataSource) canBeWritten() bool {
	switch d {
	case adDataSourceLocal, adDataSourceExternal, adDataSourceProvider:
		return true
	default:
		return false
	}
}

func (m adMirror) read(ctx context.Context, adCid cid.Cid, skipEntries bool) (adBlock *carstore.AdBlock, source adDataSource, err error) {
	if m.carReader != nil {
		adBlock, err = m.carReader.Read(ctx, adCid, skipEntries)
		if err == nil {
			return adBlock, adDataSourceLocal, nil
		}
		if m.carExternalReader == nil || !errors.Is(err, fs.ErrNotExist) {
			return nil, adDataSourceNone, err
		}
		// Local miss: try External below. Keep err for fallback failure handling.
	}

	if m.carExternalReader != nil {
		adBlock2, err2 := m.carExternalReader.Read(ctx, adCid, skipEntries)
		if err2 != nil && err != nil {
			// Prefer the original Local miss over External errors so ingestion
			// is not interrupted by External issues. Essential external errors
			// are still looged.
			if !errors.Is(err2, fs.ErrNotExist) {
				log.Warnw("Cannot read advertisement from external filestore", "err", err2, "carPath", adCid)
			}
			return nil, adDataSourceNone, err
		} else if err2 != nil {
			return nil, adDataSourceNone, err2
		}

		return adBlock2, adDataSourceExternal, nil
	}

	return nil, adDataSourceNone, fs.ErrNotExist
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, noOverwrite bool) (*filestore.File, error) {
	return m.carWriter.Write(ctx, adCid, skipEntries, noOverwrite)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.carWriter.WriteHead(ctx, adCid, publisher)
}

func newMirror(cfgMirror config.Mirror, dstore datastore.Batching) (adMirror, error) {
	var m adMirror
	mode := cfgMirror.LocalMode
	if !mode.Valid() {
		return m, fmt.Errorf("invalid AdvertisementMirror.LocalMode %q", cfgMirror.LocalMode)
	}

	// LocalMode only controls the Local mirror.
	if mode.CanWrite() {
		switch writeStore, err := filestore.MakeFilestore(cfgMirror.Local); {
		case err != nil:
			return m, fmt.Errorf("cannot create car file storage for mirror: %w", err)
		case writeStore != nil:
			m.carWriter, err = carstore.NewWriter(dstore, writeStore, carstore.WithCompress(cfgMirror.Compress))
			if err != nil {
				return m, fmt.Errorf("cannot create mirror car file writer: %w", err)
			}
		default:
			log.Warnw("Mirror write is enabled with no storage backend", "backendType", cfgMirror.Local.Type)
		}
	}
	if mode.CanRead() {
		switch readStore, err := filestore.MakeFilestore(cfgMirror.Local); {
		case err != nil:
			return m, fmt.Errorf("cannot create car file retrieval for mirror: %w", err)
		case readStore != nil:
			m.carReader, err = carstore.NewReader(readStore, carstore.WithCompress(cfgMirror.Compress))
			if err != nil {
				return m, fmt.Errorf("cannot create mirror car file reader: %w", err)
			}
			m.exposableFilestore = readStore
		default:
			log.Warnw("Mirror read is enabled with no local backend", "backendType", cfgMirror.Local.Type)
		}
	}

	// External is independent of LocalMode: when configured it always provides
	// a read source (sole source if Local read is off, otherwise a fallback).
	if cfgMirror.External.Type != "" && cfgMirror.External.Type != "none" {
		switch {
		case mode.Enabled() && reflect.DeepEqual(cfgMirror.External, cfgMirror.Local):
			log.Warnf("External retrieval cannot be the same as the local backend, disabling external retrievals")

		default:
			externalReadStore, err := filestore.MakeFilestore(cfgMirror.External)
			if err != nil {
				return m, fmt.Errorf("cannot create external car file retrieval for mirror: %w", err)
			}

			if externalReadStore != nil {
				m.carExternalReader, err = carstore.NewReader(externalReadStore, carstore.WithCompress(cfgMirror.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file external reader: %w", err)
				}
			}
		}
	}

	// Local read and write share one backend, so readwrite mode means same store.
	m.rdWrSame = m.carWriter != nil && m.carReader != nil
	return m, nil
}
