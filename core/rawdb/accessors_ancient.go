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

// Ancient table names used by the chain freezer. Keep these in rawdb so
// accessors, freezer writers, and snapshot installers share one source of
// truth for the append-only table layout.
const (
	AncientBlocksTable     = "bodies"
	AncientTxInfosTable    = "tx_infos"
	AncientStateRootsTable = "state_roots"
)

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

// AncientStatsReader is an optional diagnostic extension for AncientReader
// implementations backed by local freezer files.
type AncientStatsReader interface {
	Stats() (freezer.Stats, error)
}

// AncientTransactionIndexReader is the optional immutable fingerprint index
// attached to the local ancient store.
type AncientTransactionIndexReader interface {
	TransactionIndexCandidates(hash [32]byte) ([]uint64, error)
	TransactionIndexCoverage() uint64
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

// NewFallbackAncientReader composes ancient readers in priority order. It is
// useful when local freezer files should be tried first, with verified cold
// snapshot files acting as the historical fallback after the local virtual tail
// has advanced.
func NewFallbackAncientReader(readers ...AncientReader) AncientReader {
	filtered := make([]AncientReader, 0, len(readers))
	for _, reader := range readers {
		if reader != nil {
			filtered = append(filtered, reader)
		}
	}
	switch len(filtered) {
	case 0:
		return NoopAncient{}
	case 1:
		return filtered[0]
	default:
		return fallbackAncientReader{readers: filtered}
	}
}

type fallbackAncientReader struct {
	readers []AncientReader
}

func (r fallbackAncientReader) TransactionIndexCandidates(hash [32]byte) ([]uint64, error) {
	var out []uint64
	for _, reader := range r.readers {
		indexed, ok := reader.(AncientTransactionIndexReader)
		if !ok || indexed.TransactionIndexCoverage() == 0 {
			continue
		}
		candidates, err := indexed.TransactionIndexCandidates(hash)
		if err != nil {
			return nil, err
		}
		out = append(out, candidates...)
	}
	return out, nil
}

func (r fallbackAncientReader) TransactionIndexCoverage() uint64 {
	var coverage uint64
	for _, reader := range r.readers {
		if indexed, ok := reader.(AncientTransactionIndexReader); ok && indexed.TransactionIndexCoverage() > coverage {
			coverage = indexed.TransactionIndexCoverage()
		}
	}
	return coverage
}

func (r fallbackAncientReader) Ancient(kind string, number uint64) ([]byte, error) {
	for _, reader := range r.readers {
		data, err := reader.Ancient(kind, number)
		if err == nil {
			return data, nil
		}
		if errors.Is(err, ErrNotInAncient) {
			continue
		}
		return nil, err
	}
	return nil, ErrNotInAncient
}

func (r fallbackAncientReader) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}
	var (
		out        [][]byte
		totalBytes uint64
	)
	for i := uint64(0); i < count; i++ {
		number := start + i
		if number < start {
			break
		}
		data, err := r.Ancient(kind, number)
		if err != nil {
			if len(out) > 0 && errors.Is(err, ErrNotInAncient) {
				break
			}
			return nil, err
		}
		if maxBytes > 0 && len(out) > 0 && totalBytes+uint64(len(data)) > maxBytes {
			break
		}
		out = append(out, data)
		totalBytes += uint64(len(data))
	}
	if len(out) == 0 {
		return nil, ErrNotInAncient
	}
	return out, nil
}

func (r fallbackAncientReader) AncientCount(kind string) (uint64, error) {
	var max uint64
	for _, reader := range r.readers {
		count, err := reader.AncientCount(kind)
		if err != nil {
			return 0, err
		}
		if count > max {
			max = count
		}
	}
	return max, nil
}

func (r fallbackAncientReader) HasAncient(kind string, number uint64) (bool, error) {
	for _, reader := range r.readers {
		ok, err := reader.HasAncient(kind, number)
		if err != nil {
			if errors.Is(err, ErrNotInAncient) {
				continue
			}
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (r fallbackAncientReader) Stats() (freezer.Stats, error) {
	for _, reader := range r.readers {
		statsReader, ok := reader.(AncientStatsReader)
		if !ok {
			continue
		}
		return statsReader.Stats()
	}
	return freezer.Stats{}, ErrNotInAncient
}
