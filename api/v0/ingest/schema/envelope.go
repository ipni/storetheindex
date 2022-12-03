package schema

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("indexer/schema")

const (
	adSignatureCodec  = "/indexer/ingest/adSignature"
	adSignatureDomain = "indexer"
	epSignatureCodec  = "/indexer/ingest/extendedProviderSignature"
)

type advSignatureRecord struct {
	domain *string
	codec  []byte
	advID  []byte
}

func (r *advSignatureRecord) Domain() string {
	if r.domain != nil {
		return *r.domain
	}
	return adSignatureDomain
}

func (r *advSignatureRecord) Codec() []byte {
	if r.codec != nil {
		return r.codec
	}
	return []byte(adSignatureCodec)
}

func (r *advSignatureRecord) MarshalRecord() ([]byte, error) {
	return r.advID, nil
}

func (r *advSignatureRecord) UnmarshalRecord(buf []byte) error {
	r.advID = buf
	return nil
}

type epSignatureRecord struct {
	domain  *string
	codec   []byte
	payload []byte
}

func (r *epSignatureRecord) Domain() string {
	if r.domain != nil {
		return *r.domain
	}
	return adSignatureDomain
}

func (r *epSignatureRecord) Codec() []byte {
	if r.codec != nil {
		return r.codec
	}
	return []byte(epSignatureCodec)
}

func (r *epSignatureRecord) MarshalRecord() ([]byte, error) {
	return r.payload, nil
}

func (r *epSignatureRecord) UnmarshalRecord(buf []byte) error {
	r.payload = buf
	return nil
}

// signaturePayload generates the data payload used to compute the Advertisement.Signature.
func signaturePayload(ad *Advertisement, oldFormat bool) ([]byte, error) {
	bindex := cid.Undef.Bytes()
	if ad.PreviousID != nil {
		bindex = ad.PreviousID.(cidlink.Link).Cid.Bytes()
	}
	ent := ad.Entries.(cidlink.Link).Cid.Bytes()

	var addrsLen int
	for _, addr := range ad.Addresses {
		addrsLen += len(addr)
	}

	// Signature data is previousID+entries+metadata+isRm
	var sigBuf bytes.Buffer
	sigBuf.Grow(len(bindex) + len(ent) + len(ad.Provider) + addrsLen + len(ad.Metadata) + 1)
	sigBuf.Write(bindex)
	sigBuf.Write(ent)
	sigBuf.WriteString(ad.Provider)
	for _, addr := range ad.Addresses {
		sigBuf.WriteString(addr)
	}
	sigBuf.Write(ad.Metadata)
	if ad.IsRm {
		sigBuf.WriteByte(1)
	} else {
		sigBuf.WriteByte(0)
	}

	// Generates the old (incorrect) data payload used for signature.  This is
	// only for compatibility with existing advertisements that have the old
	// signatures, and should be removed when no longer needed.
	if oldFormat {
		return multihash.Encode(sigBuf.Bytes(), multihash.SHA2_256)
	}

	return multihash.Sum(sigBuf.Bytes(), multihash.SHA2_256, -1)
}

// extendedSignaturePayload generates the data payload used to compute the signature for a provider from the ExtendedProviders list.
// This payload *doesn't* contain enough information to sign the ad itself.
func extendedProviderSignaturePayload(ad *Advertisement, p *Provider) ([]byte, error) {
	if ad.IsRm {
		return nil, fmt.Errorf("rm ads are not supported for extended provider signatures")
	}

	bindex := cid.Undef.Bytes()
	if ad.PreviousID != nil {
		bindex = ad.PreviousID.(cidlink.Link).Cid.Bytes()
	}
	ent := ad.Entries.(cidlink.Link).Cid.Bytes()

	// Extended signature is an authrorisation for the publisher of the main Ad to publish on behalf of the signee.
	// For more details see https://github.com/ipni/storetheindex/pull/804/files
	// The signature must contain the following fields:
	// - the main provider's identity
	// - the signee's identity addrs and metadata so that they can't be misrepresented
	// - contextID and override flag as they change the behaviour of how extended providers are interpreted (see the spec)

	var sigBuf bytes.Buffer

	var addrsLen int
	for _, addr := range p.Addresses {
		addrsLen += len(addr)
	}

	sigBuf.Grow(len(bindex) + len(ent) + len(ad.Provider) + len(ad.ContextID) + len(p.ID) + addrsLen + len(p.Metadata) + 1)
	sigBuf.Write(bindex)
	sigBuf.Write(ent)
	sigBuf.WriteString(ad.Provider)
	sigBuf.Write(ad.ContextID)
	sigBuf.WriteString(p.ID)
	for _, addr := range p.Addresses {
		sigBuf.WriteString(addr)
	}
	sigBuf.Write(p.Metadata)
	if ad.ExtendedProvider.Override {
		sigBuf.WriteByte(1)
	} else {
		sigBuf.WriteByte(0)
	}

	return multihash.Sum(sigBuf.Bytes(), multihash.SHA2_256, -1)
}

// Sign signs an advertisement using the given private key.
// This function will return an error if used to sign an ad with extended providers.
func (ad *Advertisement) Sign(key crypto.PrivKey) error {
	if ad.ExtendedProvider != nil {
		return fmt.Errorf("the ad can not be signed because it has extended providers")
	}
	return ad.signAd(key)
}

func (ad *Advertisement) signAd(key crypto.PrivKey) error {
	advID, err := signaturePayload(ad, false)
	if err != nil {
		return err
	}
	envelope, err := record.Seal(&advSignatureRecord{advID: advID}, key)
	if err != nil {
		return err
	}

	sig, err := envelope.Marshal()
	if err != nil {
		return err
	}
	ad.Signature = sig
	return nil
}

// SignWithExtendedProviders signs an advertisement by the main provider as well as by all extended providers if they are present.
func (ad *Advertisement) SignWithExtendedProviders(key crypto.PrivKey, extendedProviderKeyFetcher func(string) (crypto.PrivKey, error)) error {
	err := ad.signAd(key)
	if err != nil {
		return err
	}

	if ad.ExtendedProvider == nil {
		return nil
	}

	seenTopLevelProvider := false

	for i := range ad.ExtendedProvider.Providers {
		p := &ad.ExtendedProvider.Providers[i]
		if p.ID == ad.Provider {
			seenTopLevelProvider = true
		}

		payload, err := extendedProviderSignaturePayload(ad, p)
		if err != nil {
			return err
		}

		var privKey crypto.PrivKey
		if p.ID == ad.Provider {
			privKey = key
		} else {
			privKey, err = extendedProviderKeyFetcher(p.ID)
			if err != nil {
				return err
			}
		}

		envelope, err := record.Seal(&epSignatureRecord{payload: payload}, privKey)
		if err != nil {
			return err
		}

		sig, err := envelope.Marshal()
		if err != nil {
			return err
		}

		p.Signature = sig
	}

	if !seenTopLevelProvider && len(ad.ExtendedProvider.Providers) > 0 {
		return fmt.Errorf("extended providers must contain provider from the encapsulating advertisement")
	}

	return nil
}

// VerifySignature verifies that the advertisement has been signed and
// generated correctly.  Returns the peer ID of the signer.
//
// The signer may be different than the provider ID in the advertisement, so
// the caller will need to check if the signer is allowed to sign this
// advertisement.
//
// Extended providers signatures are also verified.
func (ad *Advertisement) VerifySignature() (peer.ID, error) {
	// sigSize is the size of the current signature.  Any signature that is not
	// this size is the old signature format.
	const sigSize = 34

	// Consume envelope
	rec := &advSignatureRecord{}
	envelope, err := record.ConsumeTypedEnvelope(ad.Signature, rec)
	if err != nil {
		return "", err
	}

	// Calculate our own hash of the advertisement.
	oldFormat := len(rec.advID) != sigSize
	genID, err := signaturePayload(ad, oldFormat)
	if err != nil {
		return "", err
	}

	// Check that our own hash is equal to the hash from the signature.
	if !bytes.Equal(genID, rec.advID) {
		return "", errors.New("invalid signature")
	}

	// Get the peer ID that was used to sign the advertisement.  This may be
	// different than the provider ID, so caller will need to check if it was
	// allowed to sign this advertisement.
	signerID, err := peer.IDFromPublicKey(envelope.PublicKey)
	if err != nil {
		return "", fmt.Errorf("cannot convert public key to peer ID: %w", err)
	}

	if oldFormat {
		log.Warnw("advertisement has deprecated signature format", "signer", signerID)
	}

	if ad.ExtendedProvider != nil {
		rec := &epSignatureRecord{}
		// The top level provider must appear in the list of extended providers otherwise the ad is considered invalid
		seenTopLevelProv := false
		for _, p := range ad.ExtendedProvider.Providers {

			_, err = record.ConsumeTypedEnvelope(p.Signature, rec)
			if err != nil {
				return "", err
			}

			// Calculate our signature payload
			genPayload, err := extendedProviderSignaturePayload(ad, &p)
			if err != nil {
				return "", err
			}

			// Check that our own hash is equal to the hash from the signature.
			if !bytes.Equal(genPayload, rec.payload) {
				return "", errors.New("invalid signature")
			}

			if p.ID == ad.Provider {
				seenTopLevelProv = true
			}
		}

		if !seenTopLevelProv && len(ad.ExtendedProvider.Providers) > 0 {
			return "", fmt.Errorf("extended providers must contain provider from the encapsulating advertisement")
		}
	}

	return signerID, nil
}
