package carstore

import "errors"

var ErrHAMT = errors.New("hamt entries not supported")

// ErrUnusable is a stable errKind for an unusable CAR file. Values are
// Prometheus labels (low cardinality) and must not include CIDs or other
// per-ad data. Return a constant directly, or wrap it with fmt.Errorf("%w: ...", reason).
type ErrUnusable string

func (e ErrUnusable) Error() string { return "CAR file is unusable: " + string(e) }

const (
	ErrUnusableWrongRoot             ErrUnusable = "wrong_root"
	ErrUnusableCannotReadAd          ErrUnusable = "cannot_read_ad"
	ErrUnusableFirstBlockCidMismatch ErrUnusable = "first_block_cid_mismatch"
	ErrUnusableUndefinedCID          ErrUnusable = "undefined_cid"
	ErrUnusableCIDMismatch           ErrUnusable = "cid_mismatch"
	ErrUnusableCIDHash               ErrUnusable = "cid_hash"
	ErrUnusableEntryRead             ErrUnusable = "entry_read"
	ErrUnusableNoEntries             ErrUnusable = "no_entries"
	ErrUnusableExtraEntries          ErrUnusable = "extra_entries"
	ErrUnusableUnexpectedEntry       ErrUnusable = "unexpected_entry"
	ErrUnusableDecodeEntry           ErrUnusable = "decode_entry"
	ErrUnusableIncompleteEntries     ErrUnusable = "incomplete_entries"
	ErrUnusableInvalidCAR            ErrUnusable = "invalid_car"
	ErrUnusableDecompress            ErrUnusable = "decompress"
)
