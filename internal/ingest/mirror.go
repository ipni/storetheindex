package ingest

import (
	"context"
	"fmt"

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
}

func (m adMirror) canRead() bool {
	return m.carReader != nil
}
func (m adMirror) canWrite() bool {
	return m.carWriter != nil
}

func (m adMirror) read(ctx context.Context, adCid cid.Cid, skipEntries bool) (*carstore.AdBlock, error) {
	return m.carReader.Read(ctx, adCid, skipEntries)
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, overWrite bool) (*filestore.File, error) {
	return m.carWriter.Write(ctx, adCid, skipEntries, overWrite)
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
	return m, nil
}
