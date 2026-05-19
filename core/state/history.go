package state

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// HistoryReader is the read-side API for archive-mode state history queries.
// It returns the state of an account / storage slot / contract code AS IT
// EXISTED at the start of `blockNum` — i.e. after block (blockNum-1) was
// applied but before block blockNum.
//
// Implementations:
//
//   - LiveStateHistoryReader (this file) — slice 1 stub. Ignores blockNum
//     and returns the LIVE on-disk state for any query. Lets the JSON-RPC
//     surface in slice 7 compile and exercise the call paths against the
//     existing chain DB without yet needing the rewind machinery.
//
//   - (future, slice 3) historyReader — walks the sh-a- / sh-s- delta
//     entries from HEAD back to blockNum, applying each as a rollback,
//     using the sh-i-a- / sh-i-s- inverse indexes to skip blocks that
//     didn't touch the queried key.
//
// All methods return (nil, nil) / (zero, nil) for "account/slot doesn't
// exist at that block" — they do NOT return an error for that case. A
// non-nil error means the underlying KV layer failed (DB unreachable,
// corrupted proto, etc.).
type HistoryReader interface {
	// AccountAt returns the Account proto at the start of blockNum.
	// Returns (nil, nil) if the account did not exist at that point.
	AccountAt(addr tcommon.Address, blockNum uint64) (*types.Account, error)

	// StorageAt returns the value of (addr, slot) at the start of blockNum.
	// Returns the zero hash with nil error for "slot is empty or account
	// doesn't exist at that point".
	StorageAt(addr tcommon.Address, slot tcommon.Hash, blockNum uint64) (tcommon.Hash, error)

	// CodeAt returns the contract bytecode at addr at the start of blockNum.
	// Returns nil with nil error for "account doesn't exist or had no code
	// at that point".
	CodeAt(addr tcommon.Address, blockNum uint64) ([]byte, error)
}

// LiveStateHistoryReader is the slice-1 no-op stub. It IGNORES blockNum
// and returns the live on-disk state for every query. The full per-block
// reconstruction lands in slice 3 — this stub lets the consumers (RPC
// handlers, debug tracers) compile against the HistoryReader interface
// before the delta-walking machinery exists.
//
// Backed by the raw KV reader (rather than a *StateDB) so it works without
// having to materialise an MPT — the only state surface this stub touches
// is the flat-state rawdb. That matches the spec's "non-archive operator
// gets degraded historical reads (== live reads) without paying any
// archive-mode setup cost" backwards-compat story.
type LiveStateHistoryReader struct {
	db ethdb.KeyValueReader
}

// NewLiveStateHistoryReader wraps a KV reader (typically the chain's
// disk store) as a HistoryReader that returns live state for any block.
func NewLiveStateHistoryReader(db ethdb.KeyValueReader) *LiveStateHistoryReader {
	return &LiveStateHistoryReader{db: db}
}

// AccountAt returns the live account at addr; blockNum is ignored.
func (r *LiveStateHistoryReader) AccountAt(addr tcommon.Address, _ uint64) (*types.Account, error) {
	acc := rawdb.ReadAccount(r.db, addr)
	if acc == nil {
		return nil, nil
	}
	return acc, nil
}

// StorageAt returns the live storage slot value for (addr, slot);
// blockNum is ignored. Slot values are stored as raw bytes with leading
// zeros trimmed by the contract writer — we right-align into a Hash.
func (r *LiveStateHistoryReader) StorageAt(addr tcommon.Address, slot tcommon.Hash, _ uint64) (tcommon.Hash, error) {
	raw := rawdb.ReadStorage(r.db, addr, slot)
	if len(raw) == 0 {
		return tcommon.Hash{}, nil
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	return h, nil
}

// CodeAt returns the live contract bytecode at addr; blockNum is ignored.
func (r *LiveStateHistoryReader) CodeAt(addr tcommon.Address, _ uint64) ([]byte, error) {
	code := rawdb.ReadCode(r.db, addr)
	if len(code) == 0 {
		return nil, nil
	}
	return code, nil
}

// Compile-time interface check.
var _ HistoryReader = (*LiveStateHistoryReader)(nil)
