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
	rdWrSame  bool
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

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries, cleanupOnly, noOoverwrite bool) (*filestore.File, error) {
	return m.carWriter.Write(ctx, adCid, skipEntries, cleanupOnly, noOoverwrite)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.carWriter.WriteHead(ctx, adCid, publisher)
}

func (m adMirror) readWriteSame() bool {
	return m.rdWrSame
}

func newMirror(cfgMirror config.Mirror, dstore datastore.Batching) (adMirror, error) {
	var m adMirror

	if !(cfgMirror.Read || cfgMirror.Write) {
		return m, nil
	}

	fileStore, err := filestore.MakeFilestore(cfgMirror.Storage)
	if err != nil {
		return m, fmt.Errorf("cannot create car file storage for mirror: %w", err)
	}
	if fileStore == nil {
		return m, nil
	}

	if cfgMirror.Write {
		m.carWriter, err = carstore.NewWriter(dstore, fileStore, carstore.WithCompress(cfgMirror.Compress))
		if err != nil {
			return m, fmt.Errorf("cannot create car file writer: %w", err)
		}
	}

	if cfgMirror.Read {
		m.carReader, err = carstore.NewReader(fileStore, carstore.WithCompress(cfgMirror.Compress))
		if err != nil {
			return m, fmt.Errorf("cannot create car file reader: %w", err)
		}

		if m.carWriter != nil { // TODO: && rdFileStore == wrFileStore {
			m.rdWrSame = true
		}
	}

	return m, nil
}
