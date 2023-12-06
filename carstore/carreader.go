package carstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/filestore"
	"github.com/libp2p/go-libp2p/core/peer"
)

type CarReader struct {
	compAlg   string
	fileStore filestore.Interface
}

// AdBlock contains schema.Advertisement data.
type AdBlock struct {
	Cid     cid.Cid
	Data    []byte
	Entries <-chan EntryBlock
}

func (a AdBlock) Advertisement() (schema.Advertisement, error) {
	return decodeAd(a.Data, a.Cid)
}

// EntryBlock contains schema.EntryChunk data.
type EntryBlock struct {
	Cid  cid.Cid
	Data []byte
	Err  error
}

func (e EntryBlock) EntryChunk() (*schema.EntryChunk, error) {
	chunk, err := decodeEntryChunk(e.Data, e.Cid)
	if err != nil {
		node, err := decodeIPLDNode(bytes.NewBuffer(e.Data), e.Cid.Prefix().Codec, basicnode.Prototype.Any)
		if err != nil {
			return nil, err
		}
		if isHAMT(node) {
			return nil, ErrHAMT
		}
		return nil, err
	}
	return chunk, nil
}

type gzipReadCloser struct {
	r   io.ReadCloser
	gzr *gzip.Reader
}

func (g gzipReadCloser) Read(p []byte) (n int, err error) {
	return g.gzr.Read(p)
}

func (g gzipReadCloser) Close() error {
	err := g.gzr.Close()
	if err != nil {
		g.r.Close()
		return err
	}
	return g.r.Close()
}

// NewReader creates a CarReader that reads CAR files from the given filestore
// and returns advertisements and entries.
func NewReader(fileStore filestore.Interface, options ...Option) (*CarReader, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}
	return &CarReader{
		compAlg:   opts.compAlg,
		fileStore: fileStore,
	}, nil
}

func carFilePath(adCid cid.Cid, compAlg string) string {
	carPath := adCid.String() + CarFileSuffix
	if compAlg == Gzip {
		carPath += GzipFileSuffix
	}
	return carPath
}

func (cr CarReader) CarPath(adCid cid.Cid) string {
	return carFilePath(adCid, cr.compAlg)
}

func (cr CarReader) Compression() string {
	return cr.compAlg
}

// Read reads an advertisement CAR file, identitfied by the advertisement CID
// and returns the advertisement data and a channel to read blocks of multihash
// entries. Returns fs.ErrNotExist if file is not found.
func (cr CarReader) Read(ctx context.Context, adCid cid.Cid, skipEntries bool) (*AdBlock, error) {
	carPath := cr.CarPath(adCid)
	_, r, err := cr.fileStore.Get(ctx, carPath)
	if err != nil {
		return nil, err
	}

	var rc io.ReadCloser
	if cr.compAlg == Gzip {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		rc = &gzipReadCloser{
			r:   r,
			gzr: gzr,
		}
	} else {
		rc = r
	}

	cbr, err := car.NewBlockReader(rc)
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("cannot create car blockstore: %w", err)
	}
	if len(cbr.Roots) == 0 || cbr.Roots[0] != adCid {
		rc.Close()
		return nil, errors.New("car file has wrong root")
	}

	blk, err := cbr.Next()
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("cannot read advertisement data: %w", err)
	}

	adBlock := AdBlock{
		Cid:  adCid,
		Data: blk.RawData(),
	}

	if !skipEntries && len(cbr.Roots) > 1 {
		entsCh := make(chan EntryBlock)
		adBlock.Entries = entsCh
		go readEntries(ctx, cbr, rc, entsCh)
	} else {
		rc.Close()
	}

	return &adBlock, nil
}

// ReadHead reads the advertisement CID from the publisher's head file. The
// head file contains the CID of the latest advertisement for an advertisement
// publisher. Returns fs.ErrNotExist if head file is not found.
func (cr CarReader) ReadHead(ctx context.Context, publisher peer.ID) (cid.Cid, error) {
	err := publisher.Validate()
	if err != nil {
		return cid.Undef, err
	}

	headPath := publisher.String() + HeadFileSuffix
	_, r, err := cr.fileStore.Get(ctx, headPath)
	if err != nil {
		return cid.Undef, err
	}
	defer r.Close()

	buf := bytes.NewBuffer(make([]byte, 0, 64))
	_, err = buf.ReadFrom(r)
	if err != nil {
		return cid.Undef, err
	}
	return cid.Decode(buf.String())
}

func readEntries(ctx context.Context, cbr *car.BlockReader, r io.ReadCloser, entsCh chan EntryBlock) {
	defer r.Close()
	defer close(entsCh)

	if ctx.Err() != nil {
		entsCh <- EntryBlock{
			Err: ctx.Err(),
		}
		return
	}

	for {
		var entBlock EntryBlock

		blk, err := cbr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			entBlock.Err = err
		} else {
			entBlock.Cid = blk.Cid()
			entBlock.Data = blk.RawData()
		}

		select {
		case entsCh <- entBlock:
		case <-ctx.Done():
			// Clear entsCh and write error.
			select {
			case <-entsCh:
			default:
			}
			entsCh <- EntryBlock{
				Err: ctx.Err(),
			}
			return
		}
	}
}
