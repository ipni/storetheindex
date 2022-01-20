package ingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p-core/peer"
)

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
