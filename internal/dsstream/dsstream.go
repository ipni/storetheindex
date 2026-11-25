// Package dsstream streams the indexer datastore's kinds of an indexer
// import out of one datastore and into another: importer.KindAdProcessed,
// importer.KindAdSkipReason, importer.KindSync and importer.KindProviders.
// A wire key, and a cursor, is a datastore key without its kind's prefix.
package dsstream

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"

	"github.com/ipfs/go-cid"
	leveldbds "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/storeutil"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// providersPrefix starts the datastore key of each provider's info
const providersPrefix = dskey.ProviderInfoPath + "/"

// frozenCheckBytes is how many bytes of keys and values the source reads
// between two checks that it is not frozen
var frozenCheckBytes = 1 << 20

// kindPrefix returns the datastore key prefix of a kind's keys
func kindPrefix(kind importer.Kind) (string, error) {
	switch kind {
	case importer.KindAdProcessed:
		return dskey.AdProcessedPrefix, nil
	case importer.KindAdSkipReason:
		return dskey.AdSkipReasonPrefix, nil
	case importer.KindSync:
		return dskey.SyncPrefix, nil
	case importer.KindProviders:
		return providersPrefix, nil
	default:
		return "", importer.PermanentError(fmt.Errorf("unknown kind %q", kind))
	}
}

// TODO: I think frozen sources could still be sync-able? Maybe we shuld skip data
// that indicates frozen ads? Not yet sure - let's deal with this in a separate step.
// errFrozen refuses every part of a frozen source. A frozen indexer writes
// an ad's marker without indexing the ad's multihashes, so its markers must
// not be copied.
var errFrozen = importer.PermanentError(errors.New("the source indexer is frozen"))

// NewSource returns the import source of an indexer's datastore. It reads
// the datastore's LevelDB directly, since a part is a range of keys from a
// cursor. It picks the end of an open part with storeutil.PlanPart, which
// reads a sample of the part, after about storeutil.DefaultPartBytes of wire
// keys and values without a size hint. Every key is its own cluster, so an
// open part is about the size of its sample (see clusterLen). It stops a
// part, open or closed, with an importer.PartialError once the part has
// yielded twice the size hint, or twice storeutil.DefaultPartBytes without
// one.
//
// It skips keys with an empty value, the markers of ads that are neither
// processed nor skipped (status 0 and 2), and each /sync entry whose ad is
// marked for resync (status 2): the destination's sync would otherwise stop
// at that ad and never process it again. A frozen source refuses with a
// permanent error, when a part is opened and while it is read.
func NewSource(ds *leveldbds.Datastore) importer.Source {
	return source{db: ds.DB}
}

type source struct {
	db *leveldb.DB
}

// OpenStream implements importer.Source. A closed part whose end is not
// after its start is a permanent error.
func (src source) OpenStream(
	ctx context.Context,
	kind importer.Kind,
	start, end []byte,
	sizeHint uint64,
) (
	[]byte,
	iter.Seq2[importer.Entry, error],
	error,
) {
	prefix, err := kindPrefix(kind)
	if err != nil {
		return nil, nil, err
	}
	if err := src.checkFrozen(); err != nil {
		return nil, nil, err
	}
	sizeHint = cmp.Or(sizeHint, storeutil.DefaultPartBytes)
	maxBytes := 2 * min(sizeHint, math.MaxUint64/2)

	if end != nil {
		// End is specified - do not do the estimation, read whole part
		if bytes.Compare(end, start) <= 0 {
			return nil, nil, importer.PermanentError(fmt.Errorf("part end %q is not after its start %q", end, start))
		}
		return end, src.streamOut(ctx, prefix, nil, &util.Range{
			Start: dsKey(prefix, start),
			Limit: dsKey(prefix, end),
		}, maxBytes), nil
	}

	// The prefix ends with '/', and '0' is the byte after it
	kindEnd := []byte(prefix[:len(prefix)-1] + "0")
	partEnd, head, next, err := storeutil.PlanPart(
		func(from []byte) iter.Seq2[importer.Entry, error] {
			return src.streamOut(ctx, prefix, nil, &util.Range{
				Start: dsKey(prefix, from),
				Limit: kindEnd,
			}, 0)
		},
		start, sizeHint, clusterLen,
	)
	if err != nil {
		return nil, nil, err
	}

	// The rest of the part after the sample, none when the sample is the
	// whole part
	var rest *util.Range
	if next != nil {
		rest = &util.Range{Start: dsKey(prefix, next), Limit: kindEnd}
		if partEnd != nil {
			rest.Limit = dsKey(prefix, partEnd)
		}
	}
	return partEnd, src.streamOut(ctx, prefix, head, rest, maxBytes), nil
}

// clusterLen is the storeutil.ClusterLenFunc of every kind: each key is its
// own cluster. The keys are CIDs and peer IDs in text, which use few of the
// byte values, so their data does not spread evenly over the keys as
// numbers.
func clusterLen(key []byte) int { return len(key) }

// dsKey returns the datastore key of a wire key or cursor of a kind with
// prefix
func dsKey(prefix string, wire []byte) []byte {
	return append([]byte(prefix), wire...)
}

// checkFrozen returns errFrozen when the datastore has the frozen key
func (src source) checkFrozen() error {
	frozen, err := src.db.Has([]byte(dskey.Frozen), nil)
	if err != nil {
		return err
	}
	if frozen {
		return errFrozen
	}
	return nil
}

// streamOut yields head, then the wire entries of the keys in rng, with
// storeutil.StreamPart; a nil rng reads none. The keys must all have
// prefix. It reads the keys with one LevelDB iterator, which it releases
// when the loop ends, and yields the iterator's keys and values without
// copying them, so an entry is valid until the next iteration. It checks
// ctx on every key it reads, including the keys it does not send.
func (src source) streamOut(
	ctx context.Context,
	prefix string,
	head []importer.Entry,
	rng *util.Range,
	maxBytes uint64,
) iter.Seq2[importer.Entry, error] {
	return storeutil.StreamPart(
		ctx,
		head,
		src.wireEntries(
			prefix,
			levelDBKVs(ctx, src.db, rng),
		),
		maxBytes,
	)
}

// levelDBKV is a key and a value as LevelDB stores them: the key starts
// with its kind's prefix
type levelDBKV struct {
	key, value []byte
}

// levelDBKVs yields the keys and values in rng of db, in key order, with one
// iterator released when the loop ends. A nil rng reads none. It checks ctx
// before every key, and ends with an error when ctx is done (its cause) or
// the read fails. A yielded key and value are the iterator's own buffers:
// they are valid until the next iteration, and the caller must not alter
// their bytes.
func levelDBKVs(
	ctx context.Context,
	db *leveldb.DB,
	rng *util.Range,
) iter.Seq2[levelDBKV, error] {
	return func(yield func(levelDBKV, error) bool) {
		if rng == nil {
			return
		}
		it := db.NewIterator(rng, nil)
		defer it.Release()

		for it.Next() {
			if err := context.Cause(ctx); err != nil {
				yield(levelDBKV{}, err)
				return
			}
			if !yield(levelDBKV{it.Key(), it.Value()}, nil) {
				return
			}
		}
		if err := it.Error(); err != nil {
			yield(levelDBKV{}, err)
		}
	}
}

// wireEntries converts the keys of kvs, which start with prefix, to wire
// entries with toWireEntry and drops the keys it does not send. It checks
// that the source is not frozen before the first key and after every
// frozenCheckBytes of keys and values read, and ends with errFrozen once it
// is. An error from kvs is yielded and ends the loop.
func (src source) wireEntries(
	prefix string,
	kvs iter.Seq2[levelDBKV, error],
) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		// read counts the bytes read since the last frozen check
		read := frozenCheckBytes
		for kv, err := range kvs {
			if err != nil {
				yield(importer.Entry{}, err)
				return
			}
			if read >= frozenCheckBytes {
				if err := src.checkFrozen(); err != nil {
					yield(importer.Entry{}, err)
					return
				}
				read = 0
			}
			read += len(kv.key) + len(kv.value)

			e, send, err := src.toWireEntry(prefix, kv.key, kv.value)
			if err != nil {
				yield(importer.Entry{}, err)
				return
			}
			if send && !yield(e, nil) {
				return
			}
		}
	}
}

// toWireEntry converts a datastore key with prefix and its value to the
// import's wire layout. entry's key is the datastore key without prefix, and
// its value is the stored value; entry shares its key and value with the
// arguments. send is false for a key that is not sent: the prefix alone, an
// empty value, a marker of an ad that is neither processed nor skipped, or a
// /sync entry whose ad is marked for resync.
func (src source) toWireEntry(
	prefix string,
	key, value []byte,
) (
	entry importer.Entry,
	send bool,
	err error,
) {
	if len(key) <= len(prefix) || len(value) == 0 {
		return importer.Entry{}, false, nil
	}

	switch prefix {
	case dskey.AdProcessedPrefix:
		send = value[0] == dskey.AdMarkerProcessed || value[0] == dskey.AdMarkerSkipped

	case dskey.SyncPrefix:
		// The value is the CID of the publisher's latest synced ad. The next
		// sync walks the publisher's ad chain from its head back to that ad
		// and stops there, without processing it.
		_, c, err := cid.CidFromBytes(value)
		if err != nil {
			// The destination reads it as the source does
			send = true
			break
		}
		// An explicit resync marks each ad it walks as resync (status 2) until
		// it processes the ad again, so the entry can point at such an ad.
		// The source sends no resync markers, so a destination given the
		// entry would stop at that ad and never process it or the older ads
		// of the resync. Without the entry, the destination walks the whole
		// chain and processes every ad that has no processed or skipped
		// marker. An ad with no marker is sent, and the destination stops at
		// it as the source does.
		status, err := src.markerStatus(c)
		if err != nil {
			return importer.Entry{}, false, err
		}
		send = status != dskey.AdMarkerResync

	default:
		send = true
	}
	return importer.Entry{Key: key[len(prefix):], Value: value}, send, nil
}

// markerStatus returns the status byte of an ad's marker, 0 when it has none
func (src source) markerStatus(c cid.Cid) (byte, error) {
	v, err := src.db.Get([]byte(dskey.AdProcessedPrefix+c.String()), nil)
	if errors.Is(err, leveldb.ErrNotFound) || (err == nil && len(v) == 0) {
		return dskey.AdMarkerUnprocessed, nil
	}
	if err != nil {
		return 0, err
	}
	return v[0], nil
}
