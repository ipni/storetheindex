package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/go-indexer-core"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type adCacheItem struct {
	value indexer.Value
	isRm  bool
}

var (
	errBadAdvert              = errors.New("bad advertisement")
	errInvalidAdvertSignature = errors.New("invalid advertisement signature")
)

func dsKey(k string) datastore.Key {
	return datastore.NewKey(k)
}

// mkLinkSystem makes the indexer linkSystem which checks advertisement
// signatures at storage. If the signature is not valid the traversal/exchange
// is terminated.
func mkLinkSystem(ds datastore.Batching, reg *registry.Registry) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, dsKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}

	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			origBuf := buf.Bytes()

			log := log.With("cid", c)

			// Decode the node to check its type.
			n, err := decodeIPLDNode(buf)
			if err != nil {
				log.Errorw("Error decoding IPLD node in linksystem", "err", err)
				return errors.New("bad ipld data")
			}
			// If it is an advertisement.
			if isAdvertisement(n) {
				log.Infow("Received advertisement")

				// Verify that the signature is correct and the advertisement
				// is valid.
				ad, provID, err := verifyAdvertisement(n)
				if err != nil {
					return err
				}

				addrs, err := schema.IpldToGoStrings(ad.FieldAddresses())
				if err != nil {
					log.Errorw("Could not get addresses from advertisement", err)
					return errBadAdvert
				}

				// Register provider or update existing registration.  The
				// provider must be allowed by policy to be registered.
				err = reg.RegisterOrUpdate(lctx.Ctx, provID, addrs, c)
				if err != nil {
					return err
				}

				// Persist the advertisement.  This is read later when
				// processing each chunk of entries, to get info common to all
				// entries in a chunk.
				return ds.Put(lctx.Ctx, dsKey(c.String()), origBuf)
			}
			log.Debug("Received IPLD node")
			// Any other type of node (like entries) are stored right away.
			return ds.Put(lctx.Ctx, dsKey(c.String()), origBuf)
		}, nil
	}
	return lsys
}

func decodeAd(n ipld.Node) (schema.Advertisement, error) {
	nb := schema.Type.Advertisement.NewBuilder()
	err := nb.AssignNode(n)
	if err != nil {
		return nil, err
	}
	return nb.Build().(schema.Advertisement), nil
}

func verifyAdvertisement(n ipld.Node) (schema.Advertisement, peer.ID, error) {
	ad, err := decodeAd(n)
	if err != nil {
		log.Errorw("Cannot decode advertisement", "err", err)
		return nil, peer.ID(""), errBadAdvert
	}
	// Verify advertisement signature
	signerID, err := schema.VerifyAdvertisement(ad)
	if err != nil {
		// stop exchange, verification of signature failed.
		log.Errorw("Advertisement signature verification failed", "err", err)
		return nil, peer.ID(""), errInvalidAdvertSignature
	}

	// Get provider ID from advertisement.
	provID, err := providerFromAd(ad)
	if err != nil {
		log.Errorw("Cannot get provider from advertisement", "err", err)
		return nil, peer.ID(""), errBadAdvert
	}

	// Verify that the advertised provider has signed, and
	// therefore approved, the advertisement regardless of who
	// published the advertisement.
	if signerID != provID {
		// TODO: Have policy that allows a signer (publisher) to
		// sign advertisements for certain providers.  This will
		// allow that signer to add, update, and delete indexed
		// content on behalf of those providers.
		log.Errorw("Advertisement not signed by provider", "provider", provID, "signer", signerID)
		return nil, peer.ID(""), errInvalidAdvertSignature
	}

	return ad, provID, nil
}

func providerFromAd(ad schema.Advertisement) (peer.ID, error) {
	provider, err := ad.FieldProvider().AsString()
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot read provider from advertisement: %s", err)
	}

	providerID, err := peer.Decode(provider)
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot decode provider peer id: %s", err)
	}

	return providerID, nil
}

// IndexContentBlock indexes the content CIDs in a block of data.  First the
// advertisement is loaded to get the context ID and metadata, and then that
// and the CIDs in the content block are indexed by the indexer-core.
//
// The pubID is the peer ID of the message publisher.  This is not necessarily
// the same as the provider ID in the advertisement.  The publisher is the
// source of the indexed content, the provider is where content can be
// retrieved from.  It is the provider ID that needs to be stored by the
// indexer.
func (ing *Ingester) IndexContentBlock(adCid cid.Cid, ad schema.Advertisement, pubID peer.ID, nentries ipld.Node) error {
	// Decode the list of cids into a List_String
	nb := schema.Type.EntryChunk.NewBuilder()
	err := nb.AssignNode(nentries)
	if err != nil {
		return fmt.Errorf("cannot decode entries: %s", err)
	}

	nchunk := nb.Build().(schema.EntryChunk)

	// If this entry chunk hash a next link, add a mapping from the next
	// entries CID to the ad CID so that the ad can be loaded when that chunk
	// is received.
	//
	// Do not return here if error; try to index content in this chunk.
	hasNextLink, linkErr := ing.setNextCidToAd(nchunk, adCid)

	// Load the advertisement data for this chunk.  If there are more chunks to
	// follow, then cache the ad data.
	value, isRm, err := ing.loadAdData(adCid, hasNextLink)
	if err != nil {
		return err
	}

	mhChan := make(chan multihash.Multihash, ing.batchSize)
	// The isRm parameter is passed in for an advertisement that contains
	// entries, to allow for removal of individual entries.
	errChan := ing.batchIndexerEntries(mhChan, value, isRm)

	// Iterate over all entries and ingest (or remove) them.
	entries := nchunk.FieldEntries()
	cit := entries.ListIterator()
	var count int
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		if err != nil {
			close(mhChan)
			return fmt.Errorf("cannot decode an entry from the ingestion list: %s", err)
		}

		select {
		case mhChan <- h:
		case err = <-errChan:
			return err
		}

		count++
	}
	close(mhChan)
	err = <-errChan
	if err != nil {
		if isRm {
			return fmt.Errorf("cannot remove multihashes from indexer: %s", err)
		}
		return fmt.Errorf("cannot put multihashes into indexer: %s", err)
	}

	return linkErr
}

func (ing *Ingester) loadAdData(adCid cid.Cid, keepCache bool) (indexer.Value, bool, error) {
	ing.adCacheMutex.Lock()
	adData, ok := ing.adCache[adCid]
	if !keepCache && ok {
		if len(ing.adCache) == 1 {
			ing.adCache = nil
		} else {
			delete(ing.adCache, adCid)
		}
	}
	ing.adCacheMutex.Unlock()

	if ok {
		return adData.value, adData.isRm, nil
	}

	// Getting the advertisement for the entries so we know
	// what metadata and related information we need to use for ingestion.
	adb, err := ing.ds.Get(context.Background(), dsKey(adCid.String()))
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot read advertisement for entry from datastore: %s", err)
	}
	// Decode the advertisement.
	adn, err := decodeIPLDNode(bytes.NewBuffer(adb))
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decode ipld node: %s", err)
	}
	ad, err := decodeAd(adn)
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decode advertisement: %s", err)
	}
	// Fetch data of interest.
	contextID, err := ad.FieldContextID().AsBytes()
	if err != nil {
		return indexer.Value{}, false, err
	}
	metadataBytes, err := ad.FieldMetadata().AsBytes()
	if err != nil {
		return indexer.Value{}, false, err
	}
	isRm, err := ad.FieldIsRm().AsBool()
	if err != nil {
		return indexer.Value{}, false, err
	}

	// The peerID passed into the storage hook is the source of the
	// advertisement (the publisher), and not necessarily the same as the
	// provider in the advertisement.  Read the provider from the advertisement
	// to create the indexed value.
	providerID, err := providerFromAd(ad)
	if err != nil {
		return indexer.Value{}, false, err
	}

	// Check for valid metadata
	err = new(v0.Metadata).UnmarshalBinary(metadataBytes)
	if err != nil {
		return indexer.Value{}, false, fmt.Errorf("cannot decoding metadata: %s", err)
	}

	value := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadataBytes,
	}

	if keepCache {
		ing.adCacheMutex.Lock()
		if ing.adCache == nil {
			ing.adCache = make(map[cid.Cid]adCacheItem)
		}
		ing.adCache[adCid] = adCacheItem{
			value: value,
			isRm:  isRm,
		}
		ing.adCacheMutex.Unlock()
	}

	return value, isRm, nil
}

// batchIndexerEntries starts a goroutine that processes batches of multihashes
// from an input channels.  The goroutine collects these into a slice, storing
// up to batchSize elements.  When the slice is at capacity, a Put or Remove
// request is made to the indexer core depending on the whether isRm is true
// or false.  This function returns an error channel that returns an error if
// one occurs during processing.  This also indicates the goroutine has exited
// (and will no longer read its input channel).
//
// The goroutine exits when the input channel is closed.  It closes the error
// channel to indicate completion.
func (ing *Ingester) batchIndexerEntries(mhChan <-chan multihash.Multihash, value indexer.Value, isRm bool) <-chan error {
	var indexFunc func(indexer.Value, ...multihash.Multihash) error
	var opName string
	if isRm {
		indexFunc = ing.indexer.Remove
		opName = "remove"
	} else {
		indexFunc = ing.indexer.Put
		opName = "put"
	}

	errChan := make(chan error, 1)

	go func(batchSize int) {
		defer close(errChan)
		batch := make([]multihash.Multihash, 0, batchSize)
		var count int
		for m := range mhChan {
			batch = append(batch, m)
			if len(batch) == batchSize {
				// Process full batch of multihashes
				if err := indexFunc(value, batch...); err != nil {
					errChan <- err
					return
				}
				batch = batch[:0]
				count += batchSize
			}
		}

		if len(batch) != 0 {
			// Process any remaining puts
			if err := indexFunc(value, batch...); err != nil {
				errChan <- err
				return
			}
			count += len(batch)
		}

		log.Infow("Processed multihashes in entry chunk", "count", count, "operation", opName)
	}(ing.batchSize)

	return errChan
}

// decodeIPLDNode decodes an ipld.Node from bytes read from an io.Reader.
func decodeIPLDNode(r io.Reader) (ipld.Node, error) {
	// NOTE: Considering using the schema prototypes.  This was failing, using
	// a map gives flexibility.  Maybe is worth revisiting this again in the
	// future.
	nb := basicnode.Prototype.Any.NewBuilder()
	err := dagjson.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// Checks if an IPLD node is an advertisement, by looking to see if it has a
// "Signature" field.  We may need additional checks if we extend the schema
// with new types that are traversable.
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
}
