package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/url"
	"runtime/debug"
	"slices"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	// fuzzSyncDeadline is how long the ingestion of one fuzz input may take
	fuzzSyncDeadline = time.Minute
	// fuzzMaxOps is the number of operations read from a fuzz program, not
	// counting repetitions.
	fuzzMaxOps = 64
	// fuzzMaxBlocks and fuzzMaxBytes bound the blocks that one input builds,
	// counting a block each time it is built. They keep both the build and
	// the DAG small.
	fuzzMaxBlocks = 1 << 13
	fuzzMaxBytes  = 64 << 20
	// fuzzMaxIndexed bounds the multihashes in the entries of the ads that
	// ingestion processes, counted again for each ad that links to them.
	// Ingestion takes time in proportion to this count, and the bound keeps
	// it well within fuzzSyncDeadline while fuzz workers share the CPU.
	fuzzMaxIndexed = 1 << 20
	// fuzzMaxIdentitySize is the largest block that the identity operation
	// links by an identity CID. It is well above
	// ipnisync.MaxIdentityDigestSize, so that inputs reach both sides of the
	// limit that the indexer applies.
	fuzzMaxIdentitySize = 1024
	// fuzzKeyCount is the number of keys that a fuzz program can name as
	// provider, signer or extended provider, besides the nil key.
	fuzzKeyCount = 3
)

// Operations of a fuzz program. Each operation reads its arguments from the
// program and adds blocks to the DAG served by the publisher.
const (
	fuzzOpRaw        = iota // A block of arbitrary bytes and codec
	fuzzOpRawLarge          // A block of arbitrary bytes and codec, up to 8 MiB
	fuzzOpChunk             // An EntryChunk
	fuzzOpChunkLarge        // An EntryChunk of up to 65535 valid multihashes
	fuzzOpHamt              // A HAMT root or node, valid or not
	fuzzOpLinks             // A list of links to earlier blocks
	fuzzOpAd                // A signed advertisement
	fuzzOpIdentity          // Link the next block with an identity CID
	fuzzOpRepeat            // Run the previous operation again, many times
	fuzzOpCount
)

// FuzzIngest ingests an advertisement chain built from the fuzz input. The
// input is a program whose operations build blocks of any bytes, entry
// chunks, HAMT nodes, lists of links and signed advertisements, each of which
// can link to any block built before it. An HTTP publisher serves each
// block's bytes as they are and announces the last advertisement as its
// head. The test checks these invariants:
//
//   - ingestion does not panic and finishes within fuzzSyncDeadline;
//   - an advertisement that the indexer accepts is processed, indexed or
//     skipped, and not left to be retried when a retry can never succeed.
//     An ad that leaves a new provider with no valid address is the one
//     exception: a later ad may bring an address;
//   - every value indexed for a multihash comes from an accepted, non-removal
//     ad of the value's provider and context ID whose entries hold the
//     multihash, and carries the metadata of such an ad;
//   - every multihash of an ad that is processed and not skipped is
//     indexed, unless the ad is a removal or a later ad removed its context,
//     counting the multihashes that ingest indexes;
//   - every indexed multihash has a digest of at most maxDigestLength;
//   - a context whose latest processed ad is a removal has nothing indexed;
//   - every extended provider in the registry was signed for by its own key,
//     or is the ad's provider signed for by the ad's signer;
//   - no block is fetched deeper in the ad chain than AdvertisementDepthLimit,
//     or deeper below an ad's entries than EntriesDepthLimit.
//
// Ingestion that does not finish in time panics with the stacks of all
// goroutines, since the stuck ingestion would keep running in the fuzzing
// process.
func FuzzIngest(f *testing.F) {
	for _, seed := range fuzzSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, program []byte) {
		dag := buildFuzzDag(t, program)
		if dag.head == cid.Undef {
			return
		}
		runFuzzIngest(t, dag)
	})
}

// fuzzBlock is what the test knows about a block it built
type fuzzBlock struct {
	// links is the set of CIDs of the blocks that this block links to: the
	// Next of an entry chunk, the child nodes of a HAMT node, the links in a
	// list of links, or the PreviousID and Entries of an advertisement.
	links map[cid.Cid]struct{}
	// mhs is the set of multihashes that the block lists for indexing, keyed
	// by their bytes: the entries of an entry chunk or the bucket keys of a
	// HAMT node.
	mhs map[string]struct{}
	// ad is the advertisement that the block holds, or nil if the block is
	// not an advertisement.
	ad *fuzzAd
}

// fuzzAd is an advertisement built by a fuzz program
type fuzzAd struct {
	// ad is the advertisement as published, with its signatures
	ad schema.Advertisement
	// validEPs is the set of IDs of the extended providers that are signed
	// for by their own key, or that are the ad's provider signed for by the
	// ad's signer.
	validEPs map[string]struct{}
}

// fuzzDag is the DAG built from a fuzz program
type fuzzDag struct {
	// data holds the bytes of each block, as the publisher serves them
	data map[cid.Cid][]byte
	// blocks holds what the test knows about each block
	blocks map[cid.Cid]*fuzzBlock
	// order lists the blocks in the order they were built. A link selector
	// in the program picks a block by its position in order.
	order []cid.Cid
	// lastAd is the last advertisement built, or cid.Undef if there is none
	lastAd cid.Cid
	// head is the last advertisement, or the last block if there is none
	head cid.Cid
	// publisher is the key of the publisher that announces head
	publisher crypto.PrivKey
}

// fuzzBuilder runs a fuzz program
type fuzzBuilder struct {
	// program is the part of the program that is not read yet. The read
	// methods return zeros once it is used up.
	program []byte
	// d is the DAG that the program builds
	d *fuzzDag
	// built and builtBytes count the blocks built and their size, including
	// a block built again that the DAG already holds. fuzzMaxBlocks and
	// fuzzMaxBytes bound them.
	built      int
	builtBytes int
	// keys are the keys that the program can name as provider, signer or
	// extended provider: fuzzKeyCount keys and a nil. Key 0 is the publisher's. The
	// keys are generated from a fixed seed, so that a failing input fails
	// the same way when it is run again.
	keys []crypto.PrivKey
	// identity makes the next block that is built linked by an identity CID,
	// which holds the block's bytes, if the block is at most
	// fuzzMaxIdentitySize.
	identity bool
}

func buildFuzzDag(t *testing.T, program []byte) *fuzzDag {
	r := rand.NewChaCha8([32]byte{'i', 'p', 'n', 'i'})
	keys := make([]crypto.PrivKey, fuzzKeyCount, fuzzKeyCount+1)
	for i := range keys {
		key, _, err := crypto.GenerateEd25519Key(r)
		require.NoError(t, err)
		keys[i] = key
	}
	keys = append(keys, nil)

	b := &fuzzBuilder{
		program: program,
		d: &fuzzDag{
			data:      make(map[cid.Cid][]byte),
			blocks:    make(map[cid.Cid]*fuzzBlock),
			publisher: keys[0],
		},
		keys: keys,
	}
	// The last operation other than a repeat, and the arguments it read. A
	// prevOp of fuzzOpRepeat means that no such operation has run yet.
	prevOp := byte(fuzzOpRepeat)
	var prevArgs []byte
	for range fuzzMaxOps {
		if len(b.program) == 0 || b.full() {
			break
		}
		switch op := b.u8() % fuzzOpCount; op {
		case fuzzOpRepeat: // Repeat the previous operation, many times
			n := b.u16()
			if prevOp == fuzzOpRepeat {
				// This is the first op or previous op was repeat as well
				break
			}
			// Each repeat reads the previous operation's arguments again, and
			// the program then goes on after the repeat.
			rest := b.program
			for range n {
				if b.full() {
					break
				}
				b.program = prevArgs
				b.run(t, prevOp)
			}
			b.program = rest
		default:
			args := b.program
			b.run(t, op)
			prevOp, prevArgs = op, args[:len(args)-len(b.program)]
		}
	}

	d := b.d
	switch {
	case d.lastAd != cid.Undef:
		// The head is an advertisement whenever the program built one, so
		// that blocks built after the last ad do not stop ingestion at the
		// head.
		d.head = d.lastAd
	case len(d.order) != 0:
		// With no advertisement the head is the last block. This is the only
		// way an input announces a head that is not an advertisement, which
		// tests how the indexer refuses such a head.
		d.head = d.order[len(d.order)-1]
	}
	return d
}

// u8 reads a byte from the program
func (b *fuzzBuilder) u8() byte {
	if len(b.program) == 0 {
		return 0
	}
	v := b.program[0]
	b.program = b.program[1:]
	return v
}

// u16 reads a big-endian 16-bit number from the program
func (b *fuzzBuilder) u16() int {
	hi := b.u8()
	lo := b.u8()
	return int(hi)<<8 | int(lo)
}

// bytes reads up to n bytes from the program
func (b *fuzzBuilder) bytes(n int) []byte {
	n = min(n, len(b.program))
	v := b.program[:n]
	b.program = b.program[n:]
	return v
}

// str reads a string of up to 63 bytes from the program
func (b *fuzzBuilder) str() string {
	return string(b.bytes(int(b.u8() % 64)))
}

// fill returns n bytes that repeat pattern
func (b *fuzzBuilder) fill(pattern []byte, n int) []byte {
	if len(pattern) == 0 {
		pattern = []byte{'x'}
	}
	return bytes.Repeat(pattern, n/len(pattern)+1)[:n]
}

// full reports whether the program has built fuzzMaxBlocks blocks or
// fuzzMaxBytes bytes, so that no more blocks are built. A block built again
// counts each time, so that a repeat of an operation that builds the same
// block cannot run for long.
func (b *fuzzBuilder) full() bool {
	return b.built >= fuzzMaxBlocks || b.builtBytes >= fuzzMaxBytes
}

// add adds a block of the given bytes and codec to the DAG, and returns a
// link to it. The link has a sha2-256 CID, or an identity CID if the identity
// operation came just before and the block is at most fuzzMaxIdentitySize. A block that is
// already in the DAG keeps the fuzzBlock it was first added with.
func (b *fuzzBuilder) add(t *testing.T, codec multicodec.Code, data []byte, blk *fuzzBlock) ipld.Link {
	mhType := uint64(multihash.SHA2_256)
	if b.identity && len(data) <= fuzzMaxIdentitySize {
		mhType = multihash.IDENTITY
	}
	b.identity = false
	mh, err := multihash.Sum(data, mhType, -1)
	require.NoError(t, err)
	c := cid.NewCidV1(uint64(codec), mh)
	b.built++
	b.builtBytes += len(data)
	if _, ok := b.d.blocks[c]; !ok {
		b.d.data[c] = data
		b.d.blocks[c] = blk
		b.d.order = append(b.d.order, c)
	}
	return cidlink.Link{Cid: c}
}

// encode adds the node to the DAG, encoded in dag-cbor if codec is dag-cbor
// and in dag-json otherwise, and returns a link to it.
func (b *fuzzBuilder) encode(t *testing.T, codec multicodec.Code, n datamodel.Node, blk *fuzzBlock) ipld.Link {
	var buf bytes.Buffer
	var err error
	if codec == multicodec.DagCbor {
		err = dagcbor.Encode(n, &buf)
	} else {
		err = dagjson.Encode(n, &buf)
	}
	require.NoError(t, err)
	return b.add(t, codec, buf.Bytes(), blk)
}

// codec returns the codec at position sel in codecs. A selector past the
// last codec skips the input, so that a codec appended to codecs leaves the
// inputs that build today unchanged.
func (b *fuzzBuilder) codec(t *testing.T, sel byte, codecs ...multicodec.Code) multicodec.Code {
	if int(sel) >= len(codecs) {
		t.Skip("the codec selector is past the last codec")
	}
	return codecs[sel]
}

// nodeCodec returns the codec that sel selects for an encoded node
func (b *fuzzBuilder) nodeCodec(t *testing.T, sel byte) multicodec.Code {
	return b.codec(t, sel, multicodec.DagJson, multicodec.DagCbor)
}

// link returns a link to a block that the program has already built, which
// lets an operation link only to blocks built before it, so the DAG has no
// cycles. sel selects the block:
//
//   - 0xff selects no block;
//   - 0xfe selects the block built most recently;
//   - 0xfd selects the advertisement built most recently;
//   - any other value selects the block at position sel, modulo the number of
//     blocks built, in the order they were built.
//
// link returns nil when sel selects no block, or no such block has been built
// yet.
func (b *fuzzBuilder) link(sel byte) ipld.Link {
	d := b.d
	switch {
	case sel == 0xff || len(d.order) == 0:
		return nil
	case sel == 0xfe:
		return cidlink.Link{Cid: d.order[len(d.order)-1]}
	case sel == 0xfd:
		if d.lastAd == cid.Undef {
			return nil
		}
		return cidlink.Link{Cid: d.lastAd}
	}
	return cidlink.Link{Cid: d.order[int(sel)%len(d.order)]}
}

// addMultihashes adds the multihashes to the multihashes of the block
func (blk *fuzzBlock) addMultihashes(mhs ...multihash.Multihash) {
	for _, m := range mhs {
		if blk.mhs == nil {
			blk.mhs = make(map[string]struct{})
		}
		blk.mhs[string(m)] = struct{}{}
	}
}

// addLinks adds the links that are not nil to the links of the block
func (blk *fuzzBlock) addLinks(links ...ipld.Link) {
	for _, l := range links {
		if l == nil {
			continue
		}
		if blk.links == nil {
			blk.links = make(map[cid.Cid]struct{})
		}
		blk.links[l.(cidlink.Link).Cid] = struct{}{}
	}
}

// mh reads a multihash for an entry chunk or a HAMT bucket. It may be valid
// or not.
func (b *fuzzBuilder) mh() multihash.Multihash {
	var m multihash.Multihash
	switch b.u8() % 4 {
	case 0:
		// A valid sha2-256 multihash. Its input is only 2 bytes, so the same
		// multihash often appears more than once in a program.
		m, _ = multihash.Sum(b.bytes(2), multihash.SHA2_256, -1)
	case 1:
		// Up to 63 bytes of any value, which may not decode as a multihash
		m = multihash.Multihash(b.bytes(int(b.u8() % 64)))
	case 2:
		// A sha2-256 code with a digest of up to 2047 bytes, longer than any
		// hash function produces.
		m, _ = multihash.Encode(b.fill(b.bytes(4), b.u16()%2048), multihash.SHA2_256)
	case 3:
		// An identity multihash, whose digest holds up to 63 bytes of data
		m, _ = multihash.Encode(b.bytes(int(b.u8()%64)), multihash.IDENTITY)
	}
	return m
}

// key returns the key that sel selects. A provider or extended provider that
// names the nil key takes an ID read from the program, and an ad or extended
// provider signed by the nil key is left unsigned.
func (b *fuzzBuilder) key(sel byte) crypto.PrivKey {
	return b.keys[int(sel)%len(b.keys)]
}

// keyID returns the peer ID of key
func (b *fuzzBuilder) keyID(key crypto.PrivKey) string {
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		panic(err)
	}
	return id.String()
}

// peerID returns the ID of the key that sel selects, or a string read from
// the program when sel selects the nil key.
func (b *fuzzBuilder) peerID(sel byte) string {
	if key := b.key(sel); key != nil {
		return b.keyID(key)
	}
	return b.str()
}

// addrs reads a list of up to 3 addresses, which may not be valid
func (b *fuzzBuilder) addrs() []string {
	var addrs []string
	for range b.u8() % 4 {
		addrs = append(addrs, b.str())
	}
	return addrs
}

// run runs op, which reads its arguments from the program
func (b *fuzzBuilder) run(t *testing.T, op byte) {
	switch op {
	case fuzzOpRaw:
		// A block of any bytes: a codec, a size of up to 65535 bytes, and a
		// pattern of up to 63 bytes that is repeated to fill the block.
		b.raw(t, 1)

	case fuzzOpRawLarge:
		// The same as fuzzOpRaw with the size counted in units of 128 bytes,
		// up to 8 MiB, to reach the limits on the size of a block.
		b.raw(t, 128)

	case fuzzOpChunk:
		// An entry chunk: a codec, up to 31 multihashes, and the link to the
		// next chunk.
		enc := b.u8()
		mhs := make([]multihash.Multihash, b.u8()%32)
		for i := range mhs {
			mhs[i] = b.mh()
		}
		b.chunk(t, enc, mhs, b.link(b.u8()))

	case fuzzOpChunkLarge:
		// An entry chunk of many valid multihashes, to reach the limits on
		// the size of a block and on the number of entries: a codec, a count
		// of up to 65535, a seed, and the link to the next chunk. Multihash i
		// is the sha2-256 of the seed, the number of blocks built so far and
		// i, so that a repeat builds a new chunk each time.
		enc := b.u8()
		mhs := make([]multihash.Multihash, b.u16())
		seed := binary.BigEndian.AppendUint32(b.fill(b.bytes(2), 2), uint32(b.built))
		for i := range mhs {
			mhs[i], _ = multihash.Sum(binary.BigEndian.AppendUint32(seed, uint32(i)), multihash.SHA2_256, -1)
		}
		b.chunk(t, enc, mhs, b.link(b.u8()))

	case fuzzOpHamt:
		// A HAMT node, or a root that holds one, valid or not: flags, the
		// length of the bitmap, the bitmap, and up to 15 elements. Each
		// element is a link to another node or a bucket of up to 4
		// multihashes. Flag bit 0 makes a root, bit 1 reads any bitmap length,
		// and the bits above select the codec. A root also reads its bucket
		// size.
		flags := b.u8()
		mapLen := 1 << (b.u8() % 5)
		if flags&2 != 0 {
			mapLen = int(b.u8())
		}
		hn := hamt.HashMapNode{Map: b.fill(b.bytes(mapLen), mapLen)}
		blk := &fuzzBlock{}
		for range b.u8() % 16 {
			k := b.u8()
			if lnk := b.link(b.u8()); k&1 != 0 && lnk != nil {
				hn.Data = append(hn.Data, hamt.Element{HashMapNode: &lnk})
				blk.addLinks(lnk)
				continue
			}
			bucket := make(hamt.Bucket, (k>>1)%5)
			for i := range bucket {
				m := b.mh()
				bucket[i] = hamt.BucketEntry{Key: m, Value: basicnode.NewBool(true)}
				blk.addMultihashes(m)
			}
			hn.Data = append(hn.Data, hamt.Element{Bucket: &bucket})
		}
		if flags&1 == 0 {
			b.encode(t, b.nodeCodec(t, flags>>2), bindnode.Wrap(&hn, hamt.HashMapNodePrototype.Type()).Representation(), blk)
			break
		}
		// A HAMT root is stored in its type-level form, as hamt.Builder
		// writes it.
		root := &hamt.HashMapRoot{
			HashAlg:    multicodec.Sha2_256,
			BucketSize: int(int8(b.u8())),
			Hamt:       hn,
		}
		b.encode(t, b.nodeCodec(t, flags>>2), bindnode.Wrap(root, hamt.HashMapRootPrototype.Type()), blk)

	case fuzzOpLinks:
		// A list of up to 7 links to earlier blocks: a codec and the links.
		// The list starts with the number of blocks built so far, so that the
		// same links build a new block each time.
		enc := b.u8()
		blk := &fuzzBlock{}
		n := b.u8() % 8
		node, err := qp.BuildList(basicnode.Prototype.Any, -1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Int(int64(len(b.d.order))))
			for range n {
				if lnk := b.link(b.u8()); lnk != nil {
					qp.ListEntry(la, qp.Link(lnk))
					blk.addLinks(lnk)
				}
			}
		})
		require.NoError(t, err)
		b.encode(t, b.nodeCodec(t, enc), node, blk)

	case fuzzOpAd:
		// A signed advertisement, as ad describes
		b.ad(t)

	case fuzzOpIdentity:
		// No block: the next block built is linked by an identity CID, if it
		// is small enough.
		b.identity = true
	}
}

// raw builds a block of any bytes. It reads a codec, a size that it
// multiplies by scale, and a pattern of up to 63 bytes that is repeated to
// fill the block.
func (b *fuzzBuilder) raw(t *testing.T, scale int) {
	codec := b.codec(t, b.u8(), multicodec.DagJson, multicodec.DagCbor, multicodec.Raw)
	n := b.u16() * scale
	b.add(t, codec, b.fill(b.bytes(int(b.u8()%64)), n), &fuzzBlock{})
}

// chunk builds an entry chunk of mhs that links to next, encoded with the
// codec that enc selects.
func (b *fuzzBuilder) chunk(t *testing.T, enc byte, mhs []multihash.Multihash, next ipld.Link) {
	node, err := (&schema.EntryChunk{Entries: mhs, Next: next}).ToNode()
	require.NoError(t, err)
	blk := &fuzzBlock{}
	blk.addMultihashes(mhs...)
	blk.addLinks(next)
	b.encode(t, b.nodeCodec(t, enc), node, blk)
}

// ad builds a signed advertisement. Its extended providers are each signed
// by a key the program selects, which need not be the extended provider's
// own key. The nil key leaves the ad, or the extended provider, unsigned.
func (b *fuzzBuilder) ad(t *testing.T) {
	enc := b.u8()
	ad := schema.Advertisement{Provider: b.peerID(b.u8())}
	signer := b.key(b.u8())
	ad.Addresses = b.addrs()
	ad.ContextID = b.bytes(int(b.u8() % 64))
	ad.Metadata = b.bytes(int(b.u8() % 64))
	ad.IsRm = b.u8()&1 != 0
	ad.Entries = b.link(b.u8())
	if ad.Entries == nil {
		ad.Entries = schema.NoEntries
	}
	ad.PreviousID = b.link(b.u8())

	// The keys that sign for the extended providers other than the ad's
	// provider, in order.
	var epKeys []crypto.PrivKey
	// The low 2 bits of eps are the number of extended providers. Bit 2 adds
	// an ExtendedProvider to an ad even with no providers in it.
	if eps := b.u8(); eps%4 != 0 || eps&4 != 0 {
		ad.ExtendedProvider = &schema.ExtendedProvider{Override: b.u8()&1 != 0}
		for range eps % 4 {
			p := schema.Provider{ID: b.peerID(b.u8())}
			if key := b.key(b.u8()); p.ID != ad.Provider {
				epKeys = append(epKeys, key)
			}
			p.Addresses = b.addrs()
			p.Metadata = b.bytes(int(b.u8() % 64))
			ad.ExtendedProvider.Providers = append(ad.ExtendedProvider.Providers, p)
		}
	}
	if signer != nil {
		var next int
		// An error leaves the remaining extended providers unsigned, and the
		// ad is published as it is.
		_ = ad.SignWithExtendedProviders(signer, func(string) (crypto.PrivKey, error) {
			key := epKeys[next]
			next++
			if key == nil {
				return nil, errors.New("unsigned extended provider")
			}
			return key, nil
		})
	}

	fa := &fuzzAd{ad: ad, validEPs: make(map[string]struct{})}
	if ad.ExtendedProvider != nil {
		var next int
		for _, p := range ad.ExtendedProvider.Providers {
			var key crypto.PrivKey
			if p.ID == ad.Provider {
				key = signer
			} else {
				key = epKeys[next]
				next++
			}
			if len(p.Signature) != 0 && key != nil && (p.ID == ad.Provider || b.keyID(key) == p.ID) {
				fa.validEPs[p.ID] = struct{}{}
			}
		}
	}

	node, err := ad.ToNode()
	require.NoError(t, err)
	blk := &fuzzBlock{ad: fa}
	blk.addLinks(ad.PreviousID, ad.Entries)
	lnk := b.encode(t, b.nodeCodec(t, enc), node, blk)
	b.d.lastAd = lnk.(cidlink.Link).Cid
}

func runFuzzIngest(t *testing.T, dag *fuzzDag) {
	cfg := defaultTestIngestConfig
	// The ads that the indexer syncs, in chain order from the head. An ad
	// beyond AdvertisementDepthLimit is not synced as part of the chain,
	// though it may be fetched as an entries block.
	chain := dag.adChain()
	syncedAds := chain[:min(len(chain), cfg.AdvertisementDepthLimit)]
	if dag.adsMultihashCount(syncedAds) > fuzzMaxIndexed {
		t.Skip("the ads hold more multihashes than fuzzMaxIndexed")
	}

	env := newTestIngestEnv(t, cfg, dag.publisher)
	for c, data := range dag.data {
		env.pub.put(c, data)
	}
	env.pub.setHead(dag.head)

	ctx, cancel := context.WithTimeout(t.Context(), fuzzSyncDeadline)
	defer cancel()
	syncErr := make(chan error, 1)
	go func() { syncErr <- env.sync(ctx) }()
	var err error
	select {
	case err = <-syncErr:
		env.stuck = errors.Is(err, context.DeadlineExceeded)
	case <-time.After(fuzzSyncDeadline + 10*time.Second):
		env.stuck = true
	}
	if env.stuck {
		// Ingestion does not stop on cancellation and keeps running, so the
		// input cannot be skipped. The panic stops the fuzzing process, and
		// the traceback shows where every goroutine is.
		debug.SetTraceback("all")
		panic(fmt.Sprintf("ingestion did not finish within %s", fuzzSyncDeadline))
	}

	// The ads that the indexer accepted when it fetched them, in chain order
	// from the head.
	var accepted []*fuzzAd
	var acceptedCids []cid.Cid
	for _, c := range syncedAds {
		if blk := dag.blocks[c]; blk.ad != nil && env.tmpStore.wasPut(c) {
			accepted = append(accepted, blk.ad)
			acceptedCids = append(acceptedCids, c)
			if err == nil {
				state := env.adState(t, c)
				// Sync reports the result of the head only. An older ad of
				// another provider may wait for a later ad to bring the
				// address that its new provider lacks.
				pid, _ := peer.Decode(blk.ad.ad.Provider)
				_, registered := env.reg.ProviderInfo(pid)
				waitsForAddr := !registered && len(stringsToMultiaddrs(blk.ad.ad.Addresses)) == 0
				require.Truef(t, state.Processed || waitsForAddr, "accepted advertisement not processed")
			}
		}
	}

	// An error that is not an adIngestError comes from the sync of the ad
	// chain, which the indexer refused. A request to the publisher that
	// fails, as when the fuzzing process is too busy to connect in time,
	// leaves the ad for retry, and the retry can succeed.
	var ingestErr adIngestError
	var reqErr *url.Error
	require.Falsef(t, errors.As(err, &ingestErr) && !errors.Is(err, registry.ErrMissingProviderAddr) && !errors.As(err, &reqErr),
		"accepted advertisement left for retry: %v", err)

	checkFuzzIndex(t, dag, env.core, accepted, err == nil)
	checkFuzzComplete(t, dag, env, accepted, acceptedCids, cfg.EntriesDepthLimit)
	checkFuzzExtendedProviders(t, env.reg, accepted)
	checkFuzzFetchDepth(t, dag, chain, env.pub.fetchedBlocks(), cfg.AdvertisementDepthLimit, cfg.EntriesDepthLimit)
}

// adChain returns the blocks of the ad chain from the head, following the
// PreviousID of each ad.
func (d *fuzzDag) adChain() []cid.Cid {
	var chain []cid.Cid
	seen := make(map[cid.Cid]bool)
	for c := d.head; c != cid.Undef && !seen[c]; {
		seen[c] = true
		chain = append(chain, c)
		blk := d.blocks[c]
		if blk == nil || blk.ad == nil || blk.ad.ad.PreviousID == nil {
			break
		}
		c = blk.ad.ad.PreviousID.(cidlink.Link).Cid
	}
	return chain
}

// depths returns the depth of each block reachable from root by links, with
// root at depth 1.
func (d *fuzzDag) depths(root cid.Cid) map[cid.Cid]int {
	depth := map[cid.Cid]int{root: 1}
	queue := []cid.Cid{root}
	for len(queue) != 0 {
		c := queue[0]
		queue = queue[1:]
		blk := d.blocks[c]
		if blk == nil {
			continue
		}
		for l := range blk.links {
			if _, ok := depth[l]; !ok {
				depth[l] = depth[c] + 1
				queue = append(queue, l)
			}
		}
	}
	return depth
}

// entriesMultihashes returns the set of multihashes in the entries of an
// ad, keyed by their bytes.
func (d *fuzzDag) entriesMultihashes(fa *fuzzAd) map[string]struct{} {
	mhs := make(map[string]struct{})
	if fa.ad.Entries == schema.NoEntries {
		return mhs
	}
	for c := range d.depths(fa.ad.Entries.(cidlink.Link).Cid) {
		if blk := d.blocks[c]; blk != nil {
			maps.Copy(mhs, blk.mhs)
		}
	}
	return mhs
}

// adsMultihashCount returns the number of multihashes in the entries of the
// ads in chain, counting the entries of each ad, also when ads share them.
// It ignores blocks that are not ads.
func (d *fuzzDag) adsMultihashCount(chain []cid.Cid) int {
	counts := make(map[ipld.Link]int)
	var n int
	for _, c := range chain {
		blk := d.blocks[c]
		if blk == nil || blk.ad == nil {
			continue
		}
		count, ok := counts[blk.ad.ad.Entries]
		if !ok {
			count = len(d.entriesMultihashes(blk.ad))
			counts[blk.ad.ad.Entries] = count
		}
		n += count
	}
	return n
}

func checkFuzzIndex(t *testing.T, dag *fuzzDag, core indexer.Interface, accepted []*fuzzAd, synced bool) {
	adMhs := make([]map[string]struct{}, len(accepted))
	for i, fa := range accepted {
		adMhs[i] = dag.entriesMultihashes(fa)
	}
	// The latest accepted ad of each provider and context
	latest := make(map[string]*fuzzAd)
	for _, fa := range accepted {
		k := fa.ad.Provider + "/" + string(fa.ad.ContextID)
		if _, ok := latest[k]; !ok {
			latest[k] = fa
		}
	}

	// Every multihash that any block lists
	all := make(map[string]struct{})
	for _, blk := range dag.blocks {
		maps.Copy(all, blk.mhs)
	}
	for m := range all {
		values, found, err := core.Get(multihash.Multihash(m))
		if err != nil || !found {
			continue
		}
		dm, err := multihash.Decode(multihash.Multihash(m))
		require.NoError(t, err, "undecodable multihash indexed")
		require.LessOrEqualf(t, len(dm.Digest), maxDigestLength, "multihash indexed with a digest longer than maxDigestLength")
		for _, v := range values {
			var fromAd, withMetadata bool
			for i, fa := range accepted {
				if fa.ad.Provider != v.ProviderID.String() || string(fa.ad.ContextID) != string(v.ContextID) {
					continue
				}
				_, listed := adMhs[i][m]
				fromAd = fromAd || (!fa.ad.IsRm && listed)
				withMetadata = withMetadata || string(fa.ad.Metadata) == string(v.MetadataBytes)
			}
			require.Truef(t, fromAd, "multihash indexed for provider %s and context %x by no accepted advertisement", v.ProviderID, v.ContextID)
			require.Truef(t, withMetadata, "multihash indexed with metadata of no accepted advertisement")
			if synced {
				fa := latest[v.ProviderID.String()+"/"+string(v.ContextID)]
				require.Truef(t, fa == nil || !fa.ad.IsRm, "multihash indexed for a removed context")
			}
		}
	}
}

// checkFuzzComplete checks that each accepted ad that is processed and not
// skipped has every multihash of its entries indexed, unless the ad is a
// removal or a later accepted ad removed its context. It counts the
// multihashes that ingest indexes: those that decode and whose digest is at
// least MinimumKeyLength and no longer than maxDigestLength. The first
// entries block is depth 1, and ingest syncs up to entLimit blocks below it.
// accepted and acceptedCids are in chain order from the head.
func checkFuzzComplete(t *testing.T, dag *fuzzDag, env *testIngestEnv, accepted []*fuzzAd, acceptedCids []cid.Cid, entLimit int) {
	removed := make(map[string]bool)
	for i, fa := range accepted {
		k := fa.ad.Provider + "/" + string(fa.ad.ContextID)
		if fa.ad.IsRm {
			removed[k] = true
			continue
		}
		state := env.adState(t, acceptedCids[i])
		if removed[k] || !state.Processed || state.Skipped || fa.ad.Entries == schema.NoEntries {
			continue
		}
		var mhs []multihash.Multihash
		for c, depth := range dag.depths(fa.ad.Entries.(cidlink.Link).Cid) {
			blk := dag.blocks[c]
			if blk == nil || depth > entLimit+1 {
				continue
			}
			for m := range blk.mhs {
				dm, err := multihash.Decode(multihash.Multihash(m))
				if err == nil && len(dm.Digest) >= env.ing.minKeyLen && len(dm.Digest) <= maxDigestLength {
					mhs = append(mhs, multihash.Multihash(m))
				}
			}
		}
		pid, err := peer.Decode(fa.ad.Provider)
		require.NoError(t, err)
		require.NoErrorf(t, checkAllIndexed(env.core, pid, mhs), "multihash of a processed advertisement not indexed")
	}
}

func checkFuzzExtendedProviders(t *testing.T, reg *registry.Registry, accepted []*fuzzAd) {
	valid := make(map[string]bool)
	for _, fa := range accepted {
		for ep := range fa.validEPs {
			valid[fa.ad.Provider+"/"+ep] = true
		}
	}
	for _, info := range reg.AllProviderInfo() {
		if info.ExtendedProviders == nil {
			continue
		}
		eps := slices.Clone(info.ExtendedProviders.Providers)
		for _, cxp := range info.ExtendedProviders.ContextualProviders {
			eps = append(eps, cxp.Providers...)
		}
		for _, ep := range eps {
			require.Truef(t, valid[info.AddrInfo.ID.String()+"/"+ep.PeerID.String()],
				"extended provider %s of provider %s registered without its signature", ep.PeerID, info.AddrInfo.ID)
		}
	}
}

func checkFuzzFetchDepth(t *testing.T, dag *fuzzDag, chain, fetched []cid.Cid, adLimit, entLimit int) {
	adDepth := make(map[cid.Cid]int)
	entDepth := make(map[cid.Cid]int)
	for i, c := range chain {
		adDepth[c] = i + 1
		blk := dag.blocks[c]
		if blk == nil || blk.ad == nil || blk.ad.ad.Entries == schema.NoEntries {
			continue
		}
		for e, depth := range dag.depths(blk.ad.ad.Entries.(cidlink.Link).Cid) {
			if cur, ok := entDepth[e]; !ok || depth < cur {
				entDepth[e] = depth
			}
		}
	}
	// Ingest fetches the first entries block on its own, then syncs up to
	// EntriesDepthLimit blocks below it.
	entLimit++
	for _, c := range fetched {
		ad, isAd := adDepth[c]
		ent, isEnt := entDepth[c]
		require.Truef(t, (isAd && ad <= adLimit) || (isEnt && ent <= entLimit),
			"block fetched beyond the depth limits: ad chain depth %d, entries depth %d", ad, ent)
	}
}

// fuzzProgram builds fuzz programs for the seeds
type fuzzProgram struct{ bytes.Buffer }

func (p *fuzzProgram) op(op byte, args ...byte) *fuzzProgram {
	p.WriteByte(op)
	p.Write(args)
	return p
}

// fuzzStr encodes a string argument
func fuzzStr(s string) []byte {
	return append([]byte{byte(len(s))}, s...)
}

// fuzzSeeds returns valid fuzz programs, as starting points for the fuzzer
func fuzzSeeds() [][]byte {
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	addr := cat([]byte{1}, fuzzStr("/ip4/127.0.0.1/tcp/9999"))
	var seeds [][]byte

	// An ad over an entries chain of two chunks
	p := &fuzzProgram{}
	p.op(fuzzOpChunk, 0, 2, 0, 1, 2, 0, 3, 4, 0xff)
	p.op(fuzzOpChunk, 1, 1, 0, 5, 6, 0xfe)
	p.op(fuzzOpAd, cat([]byte{0, 0, 0}, addr, fuzzStr("ctx"), fuzzStr("md"), []byte{0, 0xfe, 0xff, 0})...)
	seeds = append(seeds, bytes.Clone(p.Bytes()))

	// The same, then a removal of its context
	p.op(fuzzOpAd, cat([]byte{0, 0, 0}, addr, fuzzStr("ctx"), fuzzStr(""), []byte{1, 0xff, 0xfd, 0})...)
	seeds = append(seeds, p.Bytes())

	// An ad over a HAMT root with one bucket. The key is the sha2-256 of 0x0006,
	// whose own hash selects slot 0, where the bucket is.
	p = &fuzzProgram{}
	p.op(fuzzOpHamt, 1, 0, 0x80, 1, 2, 0xff, 0, 0, 6, 3)
	p.op(fuzzOpAd, cat([]byte{0, 0, 0}, addr, fuzzStr("ctx"), fuzzStr("md"), []byte{0, 0xfe, 0xff, 0})...)
	seeds = append(seeds, p.Bytes())

	// An ad signed by its provider, which is not the publisher, with two
	// extended providers that each sign for themselves.
	p = &fuzzProgram{}
	p.op(fuzzOpChunk, 0, 1, 0, 9, 9, 0xff)
	p.op(fuzzOpAd, cat([]byte{0, 1, 1}, addr, fuzzStr("ctx"), fuzzStr("md"), []byte{0, 0xfe, 0xff, 2, 0},
		[]byte{1, 1}, addr, fuzzStr(""),
		[]byte{2, 2}, cat([]byte{1}, fuzzStr("/ip4/127.0.0.2/tcp/9999")), fuzzStr("ep-md"))...)
	seeds = append(seeds, p.Bytes())

	// A chain of three ads over the first block, a chunk: the ad operation
	// repeated twice, each linking to the ad before it.
	p = &fuzzProgram{}
	p.op(fuzzOpChunk, 1, 1, 0, 1, 1, 0xff)
	p.op(fuzzOpAd, cat([]byte{1, 0, 0}, addr, fuzzStr("c"), fuzzStr("m"), []byte{0, 0, 0xfd, 0})...)
	p.op(fuzzOpRepeat, 0, 2)
	seeds = append(seeds, p.Bytes())

	// An ad over a chunk of 4096 generated multihashes
	p = &fuzzProgram{}
	p.op(fuzzOpChunkLarge, 1, 0x10, 0, 1, 2, 0xff)
	p.op(fuzzOpAd, cat([]byte{0, 0, 0}, addr, fuzzStr("ctx"), fuzzStr("md"), []byte{0, 0xfe, 0xff, 0})...)
	seeds = append(seeds, p.Bytes())

	return seeds
}
