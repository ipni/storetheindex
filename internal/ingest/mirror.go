package ingest

import (
	"context"
	"errors"
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

func (m adMirror) readHead(ctx context.Context, publisher peer.ID) (cid.Cid, error) {
	return m.carReader.ReadHead(ctx, publisher)
}

func (m adMirror) write(ctx context.Context, adCid cid.Cid, skipEntries bool) (*filestore.File, error) {
	return m.carWriter.Write(ctx, adCid, skipEntries)
}

func (m adMirror) writeHead(ctx context.Context, adCid cid.Cid, publisher peer.ID) (*filestore.File, error) {
	return m.carWriter.WriteHead(ctx, adCid, publisher)
}

func newMirror(cfgMirror config.Mirror, dstore datastore.Datastore) (adMirror, error) {
	var m adMirror

	if !(cfgMirror.Read || cfgMirror.Write) {
		return m, nil
	}

	fileStore, err := makeFilestore(cfgMirror.Storage)
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
	}

	return m, nil
}

// Create a new storage system of the configured type.
func makeFilestore(cfg config.FileStore) (filestore.Interface, error) {
	switch cfg.Type {
	case "local":
		return filestore.NewLocal(cfg.Local.BasePath)
	case "s3":
		return filestore.NewS3(cfg.S3.BucketName,
			filestore.WithEndpoint(cfg.S3.Endpoint),
			filestore.WithRegion(cfg.S3.Region),
			filestore.WithKeys(cfg.S3.AccessKey, cfg.S3.SecretKey),
		)
	case "":
		return nil, errors.New("storage type not defined")
	case "none":
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported file storage type: %s", cfg.Type)
}
