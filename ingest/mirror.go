package ingest

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/libp2p/go-libp2p/core/peer"
)

type adMirror struct {
	carReader *carstore.CarReader
	carWriter *carstore.CarWriter
	rdWrSame  bool
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

func (m adMirror) read(ctx context.Context, adCid cid.Cid, skipEntries bool) (*carstore.AdBlock, error) {
	return m.carReader.Read(ctx, adCid, skipEntries)
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, noOoverwrite bool) (*filestore.File, error) {
	return m.carWriter.Write(ctx, adCid, skipEntries, noOoverwrite)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.carWriter.WriteHead(ctx, adCid, publisher)
}

func (m adMirror) readWriteSame() bool {
	return m.rdWrSame
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
	}

	m.rdWrSame = m.carWriter != nil && m.carReader != nil && reflect.DeepEqual(cfgMirror.Storage, cfgMirror.Retrieval)
	return m, nil
}
