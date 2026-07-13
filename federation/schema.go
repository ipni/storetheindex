package federation

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/record"
	"github.com/multiformats/go-multicodec"
)

const (
	stateHeadRecordDomain = "indexer"
	stateHeadRecordCodec  = "/indexer/fed/stateHeadSignature"
)

var (
	Prototypes struct {
		Head                     schema.TypedPrototype
		Snapshot                 schema.TypedPrototype
		ProvidersIngestStatusMap schema.TypedPrototype
		IngestStatus             schema.TypedPrototype

		link ipld.LinkPrototype
	}

	//go:embed schema.ipldsch
	schemaBytes []byte

	_                         record.Record = (*Head)(nil)
	stateHeadRecordCodecBytes               = []byte(stateHeadRecordCodec)
)

type (
	Head struct {
		Head      ipld.Link
		Topic     *string
		PublicKey []byte
		Signature []byte
	}
	Snapshot struct {
		Epoch       uint64
		VectorClock uint64
		Providers   ProvidersIngestStatusMap
		Previous    ipld.Link
	}
	ProvidersIngestStatusMap struct {
		Keys   []string
		Values map[string]IngestStatus
	}
	IngestStatus struct {
		LastAdvertisement ipld.Link
		HeightHint        *uint64
	}
)

func init() {
	switch ts, err := ipld.LoadSchemaBytes(schemaBytes); {
	case err != nil:
		// Fatal error; IPLD schema is likely invalid.
		logger.Fatalw("Cannot load federation IPLD schema", "err", err)
	default:
		Prototypes.Head = bindnode.Prototype((*Head)(nil), ts.TypeByName("FederationHead"))
		Prototypes.Snapshot = bindnode.Prototype((*Snapshot)(nil), ts.TypeByName("FederationSnapshot"))
		Prototypes.ProvidersIngestStatusMap = bindnode.Prototype((*ProvidersIngestStatusMap)(nil), ts.TypeByName("ProvidersIngestStatusMap"))
		Prototypes.IngestStatus = bindnode.Prototype((*IngestStatus)(nil), ts.TypeByName("IngestStatus"))
	}
	Prototypes.link = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}
}

func (h *Head) Domain() string {
	return stateHeadRecordDomain
}

func (h *Head) Codec() []byte {
	return stateHeadRecordCodecBytes
}

func (h *Head) MarshalRecord() ([]byte, error) {
	hasher := sha256.New()
	if h.Head != nil {
		hasher.Write(h.Head.(cidlink.Link).Cid.Bytes())
	}
	if h.Topic != nil && *h.Topic != "" {
		hasher.Write([]byte(*h.Topic))
	}
	hasher.Write(h.PublicKey)
	return hasher.Sum(nil), nil
}

func (h *Head) UnmarshalRecord(payload []byte) error {
	if len(payload) != sha256.Size {
		return errors.New("malformed payload: unexpected length")
	}
	wantHash, err := h.MarshalRecord()
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, wantHash) {
		return errors.New("malformed payload: hashes do not match")
	}
	return nil
}

func (h *Head) Sign(signer crypto.PrivKey) error {
	seal, err := record.Seal(h, signer)
	if err != nil {
		return err
	}
	sig, err := seal.Marshal()
	if err != nil {
		return err
	}
	h.Signature = sig
	return nil
}

func (h *Head) Verify(signer crypto.PubKey) error {
	if len(h.Signature) == 0 {
		return errors.New("unsigned head")
	}

	headPubKey, err := crypto.UnmarshalPublicKey(h.PublicKey)
	if err != nil {
		return fmt.Errorf("cannot unmarshal head public key: %w", err)
	}

	switch envelope, err := record.ConsumeTypedEnvelope(h.Signature, h); {
	case err != nil:
		return err
	case !envelope.PublicKey.Equals(signer):
		return errors.New("envelope and signer public keys do not match")
	case !signer.Equals(headPubKey):
		return errors.New("head public key and signer public keys do not match")
	default:
		return nil
	}
}

func (m *ProvidersIngestStatusMap) Put(k string, v IngestStatus) {
	if _, ok := m.Values[k]; !ok {
		m.Keys = append(m.Keys, k)
	}
	m.Values[k] = v
}

func (m *ProvidersIngestStatusMap) Get(k string) (IngestStatus, bool) {
	status, ok := m.Values[k]
	return status, ok
}
