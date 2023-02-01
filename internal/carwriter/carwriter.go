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
	"github.com/ipni/storetheindex/fsutil"
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
// to CAR files.
type CarWriter struct {
	carDir string
	dstore datastore.Datastore
}

// New create a new CarWriter that reads advertisement data from the given
// datastore and writes car files to the specified directory.
func New(dstore datastore.Datastore, carDir string) (*CarWriter, error) {
	err := fsutil.DirWritable(carDir)
	if err != nil {
		return nil, err
	}

	return &CarWriter{
		carDir: carDir,
		dstore: dstore,
	}, nil
}

// WriteExisting iterates the datastore to find existing advertisements and
// writes those to CAR files. Advertisements and entries written to CAR files
// are removed from the datastore.
//
// An error writing to a CAR file stops iterating the datrastore and the error
// is returned. Errors reading advertisement data from datastore are logged and
// iteration continues.
func (cw *CarWriter) WriteExisting() (int, error) {
	adCids, err := loadAds(context.Background(), cw.dstore)
	if err != nil {
		return 0, err
	}

	if len(adCids) == 0 {
		return 0, nil
	}

	log.Infof("Writing %d advertisements from datastore to CAR files", len(adCids))

	var count int
	for _, adCid := range adCids {
		_, err = cw.Write(adCid, false)
		if err != nil {
			var werr *WriteError
			if errors.As(err, &werr) {
				return 0, err
			}
			log.Errorw("Cannot write advertisement to CAR file", "err", err)
		}
		count++
	}
	return count, nil
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
func (cw *CarWriter) Write(adCid cid.Cid, skipEntries bool) (string, error) {
	carPath := filepath.Join(cw.carDir, adCid.String()) + ".car"
	if fsutil.FileExists(carPath) {
		return carPath, cw.removeAdData(adCid, skipEntries)
	}

	carStore, err := carblockstore.OpenReadWrite(carPath, []cid.Cid{adCid})
	if err != nil {
		return "", err
	}

	err = cw.writeAdToCar(adCid, skipEntries, carStore)
	if err != nil {
		os.Remove(carPath)
		return "", err
	}

	if err = carStore.Finalize(); err != nil {
		os.Remove(carPath)
		return "", err
	}

	return carPath, nil
}

func (cw *CarWriter) deleteCids(delCids []cid.Cid) {
	for i := len(delCids) - 1; i >= 0; i-- {
		err := cw.dstore.Delete(context.Background(), datastore.NewKey(delCids[i].String()))
		if err != nil {
			log.Errorw("Error deleting advertisement data from datastore", "err", err)
		}
	}
}

func (cw *CarWriter) writeAdToCar(adCid cid.Cid, skipEntries bool, bs bstore.Blockstore) error {
	delCids := []cid.Cid{adCid}
	defer func() {
		cw.deleteCids(delCids)
	}()

	ad, data, err := cw.loadAd(adCid)
	if err != nil {
		return fmt.Errorf("cannot load advertisement: %w", err)
	}

	if err = writeBlock(adCid, data, bs); err != nil {
		delCids = nil // Do not delete ad data.
		return &WriteError{fmt.Errorf("cannot write advertisement to car file: %w", err)}
	}

	if skipEntries || ad.Entries == schema.NoEntries || ad.Entries == nil {
		return nil
	}

	entriesCid := ad.Entries.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		return errors.New("advertisement entries link is undefined")
	}

	node, data, err := cw.loadNode(entriesCid, basicnode.Prototype.Any)
	if err != nil {
		return fmt.Errorf("failed to load first entry: %w", err)
	}
	delCids = append(delCids, entriesCid)

	if isHAMT(node) {
		if err = writeBlock(entriesCid, data, bs); err != nil {
			delCids = nil // Do not delete ad data.
			return &WriteError{fmt.Errorf("cannot write entries hamt to car file: %w", err)}
		}
		return nil
	}

	for entriesCid != cid.Undef {
		chunk, data, err := cw.loadEntryChunk(entriesCid)
		if err != nil {
			return fmt.Errorf("cannot load entries block: %w", err)
		}
		if err = writeBlock(entriesCid, data, bs); err != nil {
			delCids = nil // Do not delete ad data.
			return &WriteError{fmt.Errorf("cannot write entries block to car file: %w", err)}
		}
		if chunk.Next == nil {
			break
		}
		entriesCid = chunk.Next.(cidlink.Link).Cid
		delCids = append(delCids, entriesCid)
	}

	return nil
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
		return nil, nil, fmt.Errorf("cannot fetch the node from datastore: %w", err)
	}
	node, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), prototype)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode ipldNode: %w", err)
	}
	return node, val, nil
}

func (cw *CarWriter) removeAdData(adCid cid.Cid, skipEntries bool) error {
	delCids := []cid.Cid{adCid}
	defer func() {
		cw.deleteCids(delCids)
	}()

	ad, _, err := cw.loadAd(adCid)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			delCids = nil
			return nil
		}
		return fmt.Errorf("cannot load advertisement: %w", err)
	}

	if skipEntries || ad.Entries == schema.NoEntries || ad.Entries == nil {
		return nil
	}

	entriesCid := ad.Entries.(cidlink.Link).Cid
	if entriesCid == cid.Undef {
		return errors.New("advertisement entries link is undefined")
	}

	node, _, err := cw.loadNode(entriesCid, basicnode.Prototype.Any)
	if err != nil {
		return fmt.Errorf("failed to load first entry: %w", err)
	}
	delCids = append(delCids, entriesCid)

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

func loadAds(ctx context.Context, dstore datastore.Datastore) ([]cid.Cid, error) {
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
