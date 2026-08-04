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
	mainCarReader      *carstore.CarReader
	mainCarWriter      *carstore.CarWriter
	externalCarReader  *carstore.CarReader
	exposableFilestore filestore.Interface
}

func (m adMirror) canRead() bool {
	return m.mainCarReader != nil || m.externalCarReader != nil
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
	adDataSourceExternal                     // data read from the external reader storage
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

func (m adMirror) read(ctx context.Context, adCid cid.Cid, skipEntries bool) (adBlock *carstore.AdBlock, source adDataSource, err error) {
	if m.mainCarReader != nil {
		adBlock, err = m.mainCarReader.Read(ctx, adCid, skipEntries)
		if err == nil {
			// Main hit, no need to try External
			return adBlock, adDataSourceMain, nil
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
			// err has higher priority as it indicates a failure of main mirror
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
		switch mainStore, err := filestore.MakeFilestore(cfgMirror.Main); {
		case err != nil:
			return m, fmt.Errorf("cannot create main car file store for mirror: %w", err)

		case mainStore == nil:
			log.Warnw("Main mirror is enabled with no storage backend", "backendType", cfgMirror.Main.Type)

		default:
			if cfgMirror.MainMode.CanWrite() {
				m.mainCarWriter, err = carstore.NewWriter(dstore, mainStore, carstore.WithCompress(cfgMirror.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file writer: %w", err)
				}
			}

			if cfgMirror.MainMode.CanRead() {
				m.mainCarReader, err = carstore.NewReader(mainStore, carstore.WithCompress(cfgMirror.Compress))
				if err != nil {
					return m, fmt.Errorf("cannot create mirror car file reader: %w", err)
				}
				m.exposableFilestore = mainStore
			}
		}
	}

	// External is independent of MainMode: when configured it always provides
	// a read source (sole source if Main read is off, otherwise a fallback).
	if cfgMirror.External.Type != "" && cfgMirror.External.Type != "none" {
		if cfgMirror.MainMode.Enabled() && reflect.DeepEqual(cfgMirror.External, cfgMirror.Main) {
			return m, errors.New("external retrieval cannot be the same as the main backend")
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
