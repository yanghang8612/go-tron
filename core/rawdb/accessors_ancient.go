// Package rawdb's ancient-store extension.
//
// `AncientReader` is the read fall-through that slice 2 will plumb through
// every chain accessor (ReadBlock, ReadHeader, etc.). `AncientWriter` is
// the migrate-only entry point used by the slice-3 freezing goroutine —
// hot-path code paths never touch it.
//
// Slice 1 only ships the interfaces + a noop implementation so other
// packages can start typing against `*ChainDB` immediately, before any
// accessor is migrated.

package rawdb

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

// ErrNotInAncient is returned by AncientReader implementations when the
// requested item is not (or no longer) frozen — callers should fall back
// to the hot KV store.
//
// Exported so slice-2 migration code can do `errors.Is(err, ErrNotInAncient)`
// without poking at the freezer's package-private `errOutOfBounds`.
var ErrNotInAncient = errors.New("not in ancient store")

// AncientReader exposes the subset of operations needed to read frozen
// data. Implemented by `*freezer.Freezer`; also implemented by `NoopAncient`
// for tests / configs where the freezer is disabled.
type AncientReader interface {
	// Ancient returns the raw bytes stored at the given kind/number, or
	// ErrNotInAncient if the entry is not in the freezer.
	Ancient(kind string, number uint64) ([]byte, error)

	// AncientRange returns up to count items starting at start, optionally
	// capped at maxBytes. Returns ErrNotInAncient if start is past the head.
	AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error)

	// AncientCount returns the number of items stored in the named table.
	AncientCount(kind string) (uint64, error)

	// HasAncient reports whether the named table currently stores an entry
	// at number (i.e. number is in [tail, head)).
	HasAncient(kind string, number uint64) (bool, error)
}

// AncientWriter exposes the subset of operations needed to migrate hot data
// into the freezer. Held only by the freezing goroutine (slice 3); the
// hot-path read code uses `AncientReader` exclusively.
type AncientWriter interface {
	// ModifyAncients runs fn against a write-op that appends to all tables
	// atomically. On error every appended item is rolled back.
	ModifyAncients(fn func(AncientWriteOp) error) (int64, error)

	// TruncateHead discards any frozen data above the threshold.
	// Slice 1 / 3 only — disaster-recovery path.
	TruncateHead(items uint64) (uint64, error)

	// Sync fsyncs every table to disk.
	Sync() error
}

// AncientWriteOp is the per-batch handle passed to ModifyAncients.
//
// gtron only stores pre-encoded blobs (proto.Marshal output or raw 32-byte
// state roots), so we omit geth's RLP-encoding `Append` overload — slice-2
// callers always go through `AppendRaw`.
//
// Defined as a type alias to `freezer.AncientWriteOp` (not a separately-named
// interface) so that `*freezer.Freezer.ModifyAncients`'s callback signature
// — `func(freezer.AncientWriteOp) error` — is assignable to
// `AncientWriter.ModifyAncients`'s `func(AncientWriteOp) error`. Without the
// alias the two function types differ nominally even though the method sets
// match, and slice-3's freezing goroutine would need an awkward shim.
type AncientWriteOp = freezer.AncientWriteOp

// NoopAncient is an AncientReader that always reports "no entries".
// Used by tests and by configurations that disable the freezer entirely;
// every Ancient/AncientRange call returns ErrNotInAncient and every count
// returns zero.
type NoopAncient struct{}

// Ancient always reports ErrNotInAncient.
func (NoopAncient) Ancient(string, uint64) ([]byte, error) {
	return nil, ErrNotInAncient
}

// AncientRange always reports ErrNotInAncient.
func (NoopAncient) AncientRange(string, uint64, uint64, uint64) ([][]byte, error) {
	return nil, ErrNotInAncient
}

// AncientCount always returns 0.
func (NoopAncient) AncientCount(string) (uint64, error) { return 0, nil }

// HasAncient always returns false.
func (NoopAncient) HasAncient(string, uint64) (bool, error) { return false, nil }
