// ChainDB composes gtron's hot KV store with an AncientReader so that
// chain accessors (slice 2) can transparently fall through to the freezer
// for blocks below the cutoff. Slice 1 ships the type + a constructor;
// callers still take `ethdb.KeyValueStore` directly until slice 2 migrates
// them.

package rawdb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

// ChainDB is the gtron chain database: a hot Pebble store plus an
// AncientReader fall-through for frozen rows.
//
// Embedding `ethdb.KeyValueStore` means every existing accessor that takes
// `ethdb.KeyValueReader` / `ethdb.KeyValueWriter` keeps working without
// signature changes — the new fall-through is opt-in for accessors that
// migrate in slice 2.
type ChainDB struct {
	ethdb.KeyValueStore
	AncientReader
}

// NewChainDB wraps a hot KV store and an ancient reader into a `*ChainDB`.
// `anc` may be `NoopAncient{}` when the freezer is disabled or in tests
// that don't want a freezer on disk.
func NewChainDB(kv ethdb.KeyValueStore, anc AncientReader) *ChainDB {
	if anc == nil {
		anc = NoopAncient{}
	}
	return &ChainDB{KeyValueStore: kv, AncientReader: anc}
}

// freezerReader wraps a `*freezer.Freezer` and translates the freezer's
// package-private "out of bounds" / "unknown table" errors into the public
// `ErrNotInAncient` sentinel that slice-2 accessors will key off.
//
// Slice 3 (the freezing goroutine) needs read+write access; this wrapper
// only implements the read half so it composes naturally into `ChainDB`.
type freezerReader struct {
	f *freezer.Freezer
}

// NewFreezerReader adapts a `*freezer.Freezer` so it satisfies
// `AncientReader`. Out-of-bounds reads surface as `ErrNotInAncient`.
func NewFreezerReader(f *freezer.Freezer) AncientReader {
	if f == nil {
		return NoopAncient{}
	}
	return freezerReader{f: f}
}

func (r freezerReader) Ancient(kind string, number uint64) ([]byte, error) {
	data, err := r.f.Ancient(kind, number)
	if err != nil {
		return nil, translateFreezerErr(err)
	}
	return data, nil
}

func (r freezerReader) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	out, err := r.f.AncientRange(kind, start, count, maxBytes)
	if err != nil {
		return nil, translateFreezerErr(err)
	}
	return out, nil
}

func (r freezerReader) AncientCount(kind string) (uint64, error) {
	n, err := r.f.AncientCount(kind)
	if err != nil {
		return 0, translateFreezerErr(err)
	}
	return n, nil
}

func (r freezerReader) HasAncient(kind string, number uint64) (bool, error) {
	ok, err := r.f.HasAncient(kind, number)
	if err != nil {
		return false, translateFreezerErr(err)
	}
	return ok, nil
}

// translateFreezerErr maps the freezer package's internal sentinels to
// public `core/rawdb` errors. Unknown errors pass through unchanged.
func translateFreezerErr(err error) error {
	switch {
	case errors.Is(err, freezer.ErrOutOfBounds):
		return ErrNotInAncient
	case errors.Is(err, freezer.ErrUnknownTable):
		return ErrNotInAncient
	default:
		return err
	}
}
