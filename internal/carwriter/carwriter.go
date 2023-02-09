package carwriter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	cid "github.com/ipfs/go-cid"
	datastore "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipni/storetheindex/api/v0/ingest/schema"
	"github.com/ipni/storetheindex/internal/filestore"
)

var log = logging.Logger("carwriter")

type WriteError struct {
	err error
}

func (e WriteError) Error() string {
	return e.err.Error()
}

func (e WriteError) Unwrap() error {
	return e.err
}

// CarWriter writes advertisements and entries, that are stored in a datastore,
// to CAR files. For each advertisemtns, two separate CAR files are writen. One
// for the advertisement data and one for all of the multihash entries chunks
// or HAMT. The original chunks or HAMT are preserved for the purpose of being
// able to verify links from advertisements and verify the advertisement
// signature. Such verification may be necessary when fetching car files from a
// location that is not completely trusted.
type CarWriter struct {
	dstore    datastore.Datastore
	fileStore filestore.Interface
}

// New create a new CarWriter that reads advertisement data from the given
// datastore and writes car files to the specified directory.
func New(dstore datastore.Datastore, fileStore filestore.Interface) *CarWriter {
	return &CarWriter{
		dstore:    dstore,
		fileStore: fileStore,
	}
}

// WriteExisting iterates the datastore to find existing advertisements. It
// then starts a goroutine to asynchronously write these and their entries to
// CAR files, and returns. Any advertisements added to the datastore after this
// function returns will not be handled by the goroutine. Advertisements and
// entries that are written to CAR files are removed from the datastore.
//
// An error writing to a CAR file, or context concellation, stops processing
// the advertisements from the datastore.
func (cw *CarWriter) WriteExisting(ctx context.Context) <-chan int {
	done := make(chan int, 1)

	adCids, err := findAds(context.Background(), cw.dstore)
	if err != nil {
		log.Errorw("Error loading existing advertisements from datastore", "err", err)
		close(done)
		return done
	}

	if len(adCids) == 0 {
		close(done)
		return done
	}

	go func() {
		log.Infof("Writing %d advertisements from datastore to CAR files", len(adCids))
		var count int
		for _, adCid := range adCids {
			if ctx.Err() != nil {
				break
			}
			_, _, err = cw.Write(ctx, adCid, false)
			if err != nil {
				log.Errorw("Cannot write advertisement to CAR file", "err", err)
				var werr *WriteError
				if errors.As(err, &werr) {
					// Error writing to car file; stop writing ads.
					break
				}
				// Log error, but keep going.
				continue
			}
			count++
		}
		log.Infof("Wrote %d of %d advertisements from datastore to CAR files", count, len(adCids))
		done <- count
		close(done)
	}()

	return done
}

// Write reads the advertisement, specified by CID, from the datastore and
// writes it and its entries into CAR file. The car file is stored in the
// directory specified when the CarWriter was created, and is named with the
// advertisement CID.
//
// The CAR file is written without entries if skipEntries is true. The prupose
// of this to create a car file, to maintain the link in the advertisement
// chain, when it is know that a later advertisement deletes this
// advertisement's entries.
func (cw *CarWriter) Write(ctx context.Context, adCid cid.Cid, skipEntries bool) (string, string, error) {
	ad, data, err := cw.loadAd(adCid)
	if err != nil {
		return "", "", fmt.Errorf("cannot load advertisement: %w", err)
	}

	adCarFile, err := cw.writeAdToCar(ctx, adCid, data)
	if err != nil {
		return "", "", &WriteError{err}
	}

	if skipEntries || ad.Entries == schema.NoEntries || ad.Entries == nil {
		cw.deleteCids([]cid.Cid{adCid})
		log.Infow("Wrote advertisement CAR", "adFile", adCarFile.Path, "adFileSize", adCarFile.Size)
		return adCarFile.Path, "", nil
	}

	entriesCid := ad.Entries.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		cw.deleteCids([]cid.Cid{adCid})
		return adCarFile.Path, "", nil
	}

	entCarFile, err := cw.writeEntriesToCar(ctx, adCid, entriesCid)
	if err != nil {
		return "", "", err
	}

	log.Infow("Wrote advertisement CAR", "adFile", adCarFile.Path, "adFileSize", adCarFile.Size,
		"entriesFile", entCarFile.Path, "entriesFileSize", entCarFile.Size)

	return adCarFile.Path, entCarFile.Path, nil
}

// writeAdToCar wites the advertisement data to a CAR file.
func (cw *CarWriter) writeAdToCar(ctx context.Context, adCid cid.Cid, data []byte) (*filestore.File, error) {
	fileName := adCid.String() + ".car"

	// If the destination file already exists, do not rewrite it.
	fileInfo, err := cw.fileStore.Head(ctx, fileName)
	if err == nil {
		return fileInfo, nil
	}

	carTmp := filepath.Join(os.TempDir(), fileName)

	carStore, err := carblockstore.OpenReadWrite(carTmp, []cid.Cid{adCid})
	if err != nil {
		return nil, fmt.Errorf("cannot open advertisement car file: %w", err)
	}
	defer os.Remove(carTmp)

	if err = writeBlock(adCid, data, carStore); err != nil {
		return nil, fmt.Errorf("cannot write advertisement data to car file: %w", err)
	}

	if err = carStore.Finalize(); err != nil {
		return nil, fmt.Errorf("cannot finalize advertisement car file: %w", err)
	}

	carFile, err := os.Open(carTmp)
	if err != nil {
		return nil, err
	}
	defer carFile.Close()

	return cw.fileStore.Put(ctx, fileName, carFile)
}

// writeEntriesToCar writes the entries chunks or HAMT to a car file.
func (cw *CarWriter) writeEntriesToCar(ctx context.Context, adCid, entriesCid cid.Cid) (*filestore.File, error) {
	delCids := []cid.Cid{adCid, entriesCid}

	fileName := entriesCid.String() + ".car"
	carTmp := filepath.Join(os.TempDir(), fileName)

	node, data, err := cw.loadNode(entriesCid, basicnode.Prototype.Any)
	if err != nil {
		cw.deleteCids(delCids)
		if errors.Is(err, datastore.ErrNotFound) {
			// This is not an error because an ad whose contents was deleted
			// later in the chain will have no entries in the datastore.
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load first entry: %w", err)
	}

	// If the entries file alredy exists, remove the entries from the datastore
	// and do not re-create the entries CAR file.
	fileInfo, err := cw.fileStore.Head(ctx, fileName)
	if err == nil {
		if err = cw.removeEntriesData(entriesCid, node); err != nil {
			return nil, err
		}
		return fileInfo, nil
	}

	carStore, err := carblockstore.OpenReadWrite(carTmp, []cid.Cid{entriesCid})
	if err != nil {
		return nil, &WriteError{fmt.Errorf("cannot open entries car file: %w", err)}
	}
	defer os.Remove(carTmp)

	if isHAMT(node) {
		if err = writeBlock(entriesCid, data, carStore); err != nil {
			return nil, &WriteError{fmt.Errorf("cannot write entries hamt to car file: %w", err)}
		}
	} else {
		for entriesCid != cid.Undef {
			chunk, data, err := cw.loadEntryChunk(entriesCid)
			if err != nil {
				cw.deleteCids(delCids)
				return nil, fmt.Errorf("cannot load entries block: %w", err)
			}
			if err = writeBlock(entriesCid, data, carStore); err != nil {
				return nil, &WriteError{fmt.Errorf("cannot write entries block to car file: %w", err)}
			}
			if chunk.Next == nil {
				break
			}
			entriesCid = chunk.Next.(cidlink.Link).Cid
			delCids = append(delCids, entriesCid)
		}
	}

	if err = carStore.Finalize(); err != nil {
		return nil, &WriteError{fmt.Errorf("cannot finalize entries car file: %w", err)}
	}

	carFile, err := os.Open(carTmp)
	if err != nil {
		return nil, err
	}
	defer carFile.Close()

	fileInfo, err = cw.fileStore.Put(ctx, fileName, carFile)
	if err != nil {
		return nil, err
	}

	cw.deleteCids(delCids)

	return fileInfo, nil
}

func (cw *CarWriter) deleteCids(delCids []cid.Cid) {
	for i := len(delCids) - 1; i >= 0; i-- {
		err := cw.dstore.Delete(context.Background(), datastore.NewKey(delCids[i].String()))
		if err != nil {
			log.Errorw("Error deleting advertisement data from datastore", "err", err)
		}
	}
}

// isHAMT checks if the given IPLD node is a HAMT root node by looking for a
// field named "hamt".
//
// See: https://github.com/ipld/go-ipld-adl-hamt/blob/master/schema.ipldsch
func isHAMT(n ipld.Node) bool {
	h, _ := n.LookupByString("hamt")
	return h != nil
}

func writeBlock(blockCid cid.Cid, data []byte, bs bstore.Blockstore) error {
	block, err := blocks.NewBlockWithCid(data, blockCid)
	if err != nil {
		return err
	}
	return bs.Put(context.Background(), block)
}

func (cw *CarWriter) loadAd(c cid.Cid) (schema.Advertisement, []byte, error) {
	node, data, err := cw.loadNode(c, schema.AdvertisementPrototype)
	if err != nil {
		return schema.Advertisement{}, nil, err
	}
	ad, err := schema.UnwrapAdvertisement(node)
	if err != nil {
		return schema.Advertisement{}, nil, fmt.Errorf("cannot decode advertisement: %w", err)
	}

	return *ad, data, nil
}

func (cw *CarWriter) loadEntryChunk(c cid.Cid) (*schema.EntryChunk, []byte, error) {
	node, data, err := cw.loadNode(c, schema.EntryChunkPrototype)
	if err != nil {
		return nil, nil, err
	}
	chunk, err := schema.UnwrapEntryChunk(node)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode entries chunk: %w", err)
	}
	return chunk, data, nil
}

func (cw *CarWriter) loadNode(c cid.Cid, prototype ipld.NodePrototype) (ipld.Node, []byte, error) {
	key := datastore.NewKey(c.String())
	val, err := cw.dstore.Get(context.Background(), key)
	if err != nil {
		return nil, nil, err
	}
	node, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), prototype)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode ipldNode: %w", err)
	}
	return node, val, nil
}

func (cw *CarWriter) removeEntriesData(entriesCid cid.Cid, node ipld.Node) error {
	delCids := []cid.Cid{entriesCid}
	defer func() {
		cw.deleteCids(delCids)
	}()

	if isHAMT(node) {
		return nil
	}

	for entriesCid != cid.Undef {
		chunk, _, err := cw.loadEntryChunk(entriesCid)
		if err != nil {
			return fmt.Errorf("cannot load entries block: %w", err)
		}
		if chunk.Next == nil {
			break
		}
		entriesCid = chunk.Next.(cidlink.Link).Cid
		delCids = append(delCids, entriesCid)
	}

	return nil
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(codec uint64, r io.Reader, prototype ipld.NodePrototype) (ipld.Node, error) {
	nb := prototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(codec)
	if err != nil {
		return nil, err
	}
	if err = decoder(nb, r); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// isAdvertisement checks if an IPLD node is an advertisement, by looking to
// see if it has a "Signature" field. Additional checks may be needed if the
// schema is extended with new types that are traversable.
func isAdvertisement(node ipld.Node) bool {
	indexID, _ := node.LookupByString("Signature")
	return indexID != nil
}

func findAds(ctx context.Context, dstore datastore.Datastore) ([]cid.Cid, error) {
	var q query.Query
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	var adCids []cid.Cid
	for result := range results.Next() {
		if result.Error != nil {
			return nil, fmt.Errorf("cannot read query result from datastore: %w", result.Error)
		}
		ent := result.Entry
		key := ent.Key[1:]

		// Not a CID if it contains "/".
		if strings.Contains(key, "/") {
			continue
		}
		if len(ent.Value) == 0 {
			continue
		}
		adCid, err := cid.Decode(key)
		if err != nil {
			continue
		}
		node, err := decodeIPLDNode(adCid.Prefix().Codec, bytes.NewBuffer(ent.Value), schema.AdvertisementPrototype)
		if err != nil {
			continue
		}
		if isAdvertisement(node) {
			adCids = append(adCids, adCid)
		}
	}

	return adCids, nil
}
