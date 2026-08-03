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
	localCarReader     *carstore.CarReader
	localCarWriter     *carstore.CarWriter
	externalCarReader  *carstore.CarReader
	exposableFilestore filestore.Interface
}

func (m adMirror) canRead() bool {
	return m.localCarReader != nil || m.externalCarReader != nil
}
func (m adMirror) canWrite() bool {
	return m.localCarWriter != nil
}

func (m adMirror) cleanupAdData(ctx context.Context, adCid cid.Cid, skipEntries bool) error {
	return m.localCarWriter.CleanupAdData(ctx, adCid, skipEntries)
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
		return "external"
	case adDataSourceProvider:
		return "provider"
	default:
		return fmt.Sprintf("unknown(%d)", d)
	}
}

func (d adDataSource) canBeWritten() bool {
	// Local is already on the write store; do not copy it back for rewriting.
	switch d {
	case adDataSourceExternal, adDataSourceProvider:
		return true
	default:
		return false
	}
}

func (m adMirror) read(ctx context.Context, adCid cid.Cid, skipEntries bool) (adBlock *carstore.AdBlock, source adDataSource, err error) {
	if m.localCarReader != nil {
		adBlock, err = m.localCarReader.Read(ctx, adCid, skipEntries)
		if err == nil {
			// Local hit, no need to try External
			return adBlock, adDataSourceLocal, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, adDataSourceNone, err
		}
	}

	if m.externalCarReader == nil {
		return nil, adDataSourceNone, fs.ErrNotExist
	}

	adBlock2, err2 := m.externalCarReader.Read(ctx, adCid, skipEntries)
	if err2 != nil {
		if err != nil {
			// err has higher priority as it indicates a failure of local mirror
			if !errors.Is(err2, fs.ErrNotExist) {
				log.Warnw("Cannot read advertisement from external filestore", "err", err2, "carPath", adCid)
			}
			return nil, adDataSourceNone, err
		}
		return nil, adDataSourceNone, err2
	}

	return adBlock2, adDataSourceExternal, nil
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, noOverwrite bool) (*filestore.File, error) {
	return m.localCarWriter.Write(ctx, adCid, skipEntries, noOverwrite)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.localCarWriter.WriteHead(ctx, adCid, publisher)
}

func newMirror(cfgMirror config.Mirror, dstore datastore.Batching) (m adMirror, err error) {
	if !cfgMirror.LocalMode.Valid() {
		return m, fmt.Errorf("invalid AdvertisementMirror.LocalMode %q", cfgMirror.LocalMode)
	}

	// LocalMode controls a single Local filestore shared by reader and writer.
	if cfgMirror.LocalMode.Enabled() {
		switch localStore, err := filestore.MakeFilestore(cfgMirror.Local); {
		case err != nil:
			return m, fmt.Errorf("cannot create local car file store for mirror: %w", err)

		case localStore == nil:
			log.Warnw("Local mirror is enabled with no storage backend", "backendType", cfgMirror.Local.Type)

		default:
			if cfgMirror.LocalMode.CanWrite() {
				m.localCarWriter, err = carstore.NewWriter(dstore, localStore, carstore.WithCompress(cfgMirror.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file writer: %w", err)
				}
			}

			if cfgMirror.LocalMode.CanRead() {
				m.localCarReader, err = carstore.NewReader(localStore, carstore.WithCompress(cfgMirror.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file reader: %w", err)
				}
				m.exposableFilestore = localStore
			}
		}
	}

	// External is independent of LocalMode: when configured it always provides
	// a read source (sole source if Local read is off, otherwise a fallback).
	if cfgMirror.External.Type != "" && cfgMirror.External.Type != "none" {
		if cfgMirror.LocalMode.Enabled() && reflect.DeepEqual(cfgMirror.External, cfgMirror.Local) {
			return m, errors.New("external retrieval cannot be the same as the local backend")
		}

		externalReadStore, err := filestore.MakeFilestore(cfgMirror.External)
		if err != nil {
			return m, fmt.Errorf("cannot create external car file retrieval for mirror: %w", err)
		}

		if externalReadStore != nil {
			m.externalCarReader, err = carstore.NewReader(externalReadStore, carstore.WithCompress(cfgMirror.Compress))
			if err != nil {
				return m, fmt.Errorf("cannot create mirror car file external reader: %w", err)
			}
		}
	}

	return m, nil
}
