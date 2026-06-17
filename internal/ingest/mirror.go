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
	carFallbackReader  *carstore.CarReader
	carWriter          *carstore.CarWriter
	rdWrSame           bool
	exposableFilestore filestore.Interface
}

func (m adMirror) canRead() bool {
	return m.carReader != nil
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
	adDataSourceWriter                       // data read from the same storage as the writer
	adDataSourceReader                       // data read from the reader storage that is different from the writer
	adDataSourceFallback                     // data read from the fallback reader storage
	adDataSourceProvider                     // data read from the provider
)

func (d adDataSource) String() string {
	switch d {
	case adDataSourceNone:
		return "none"
	case adDataSourceWriter:
		return "writer"
	case adDataSourceReader:
		return "reader"
	case adDataSourceFallback:
		return "fallback"
	case adDataSourceProvider:
		return "provider"
	default:
		return fmt.Sprintf("unknown(%d)", d)
	}
}

func (d adDataSource) canBeWritten() bool {
	switch d {
	case adDataSourceReader, adDataSourceFallback, adDataSourceProvider:
		return true
	default:
		return false
	}
}

func (m adMirror) read(ctx context.Context, adCid cid.Cid, skipEntries bool) (adBlock *carstore.AdBlock, source adDataSource, err error) {
	adBlock, err = m.carReader.Read(ctx, adCid, skipEntries)
	if m.carFallbackReader != nil && errors.Is(err, fs.ErrNotExist) {
		// Try the fallback reader if block is not found in the primary one
		adBlock2, err2 := m.carFallbackReader.Read(ctx, adCid, skipEntries)
		if err2 != nil {
			log.Warnw("Cannot read advertisement from fallback filestore", "err", err2, "carPath", adCid)

			// Return the original error here as we don't want to interrupt the ingestion process
			// due to issues with the fallback filestore.
			return nil, 0, err
		}

		return adBlock2, adDataSourceFallback, nil
	}
	if err != nil {
		return nil, adDataSourceNone, err
	}

	if m.rdWrSame {
		// If reader and writer storages are the same, then we mark this data as
		// read from the writer storage. This is needed to avoid writing the data
		// to the same storage as it was read from.
		return adBlock, adDataSourceWriter, nil
	}

	return adBlock, adDataSourceReader, nil
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, noOverwrite bool) (*filestore.File, error) {
	return m.carWriter.Write(ctx, adCid, skipEntries, noOverwrite)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.carWriter.WriteHead(ctx, adCid, publisher)
}

func newMirror(cfgMirror config.Mirror, dstore datastore.Batching) (adMirror, error) {
	var m adMirror
	if cfgMirror.Write {
		switch writeStore, err := filestore.MakeFilestore(cfgMirror.Storage); {
		case err != nil:
			return m, fmt.Errorf("cannot create car file storage for mirror: %w", err)
		case writeStore != nil:
			m.carWriter, err = carstore.NewWriter(dstore, writeStore, carstore.WithCompress(cfgMirror.Compress))
			if err != nil {
				return m, fmt.Errorf("cannot create mirror car file writer: %w", err)
			}
			m.exposableFilestore = writeStore
		default:
			log.Warnw("Mirror write is enabled with no storage backend", "backendType", cfgMirror.Storage.Type)
		}
	}
	if cfgMirror.Read {
		switch readStore, err := filestore.MakeFilestore(cfgMirror.Retrieval); {
		case err != nil:
			return m, fmt.Errorf("cannot create car file retrieval for mirror: %w", err)
		case readStore != nil:
			m.carReader, err = carstore.NewReader(readStore, carstore.WithCompress(cfgMirror.Compress))
			if err != nil {
				return m, fmt.Errorf("cannot create mirror car file reader: %w", err)
			}
		default:
			log.Warnw("Mirror read is enabled with no retrieval backend", "backendType", cfgMirror.Retrieval.Type)
		}

		if cfgMirror.FallbackRetrieval != nil {
			switch {
			case m.carReader == nil:
				log.Warnf("Fallback retrieval is enabled but no primary retrieval backend is configured, disabling fallback retrievals")

			case reflect.DeepEqual(*cfgMirror.FallbackRetrieval, cfgMirror.Retrieval):
				log.Warnf("Fallback retrieval cannot be the same as the primary retrieval backend, disabling fallback retrievals")

			case reflect.DeepEqual(*cfgMirror.FallbackRetrieval, cfgMirror.Storage):
				log.Warnf("Fallback retrieval cannot be the same as the storage backend, disabling fallback retrievals")

			default:
				fallbackReadStore, err := filestore.MakeFilestore(*cfgMirror.FallbackRetrieval)
				if err != nil {
					return m, fmt.Errorf("cannot create fallback car file retrieval for mirror: %w", err)
				}

				if fallbackReadStore != nil {
					m.carFallbackReader, err = carstore.NewReader(fallbackReadStore, carstore.WithCompress(cfgMirror.Compress))
					if err != nil {
						return m, fmt.Errorf("cannot create mirror car file fallback reader: %w", err)
					}
				}
			}
		}
	}

	m.rdWrSame = m.carWriter != nil && m.carReader != nil && reflect.DeepEqual(cfgMirror.Storage, cfgMirror.Retrieval)
	return m, nil
}
