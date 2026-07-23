// Freezer-side helpers used by the slice-3 background freezing goroutine.
//
// The runner package (core/freezer) needs to (a) read the raw KV bytes for
// every freezable kind at a given block number and (b) delete those rows
// from Pebble once they have been durably appended to ancient. Both
// concerns straddle the rawdb key-layout boundary, so the helpers live
// here next to the schema rather than reaching into private prefixes from
// outside the package.
//
// Slice 1's freezer scope (per the design doc) keeps `bh-<hash>` and
// `bsr-<hash>` hot in Pebble for wallet hot-path lookup. The freezer
// therefore only deletes the num-keyed rows (`b-<num>`, `tib-<num>`); the
// hash-keyed rows remain in Pebble until a future slice introduces a
// num→hash reverse index inside ancient.

package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// ReadBlockRaw returns the marshalled `corepb.Block` bytes stored under
// `b-<num>` in Pebble, or nil if no row exists. The freezer pass calls
// this on every block in the freeze range and forwards the bytes to
// `Freezer.ModifyAncients` via AppendRaw without re-encoding. Skipping the
// proto unmarshal/marshal round-trip costs the freezer goroutine about an
// order of magnitude less CPU per pass on Nile (~30 µs/block instead of
// ~300 µs/block at h≈10M).
//
// The returned slice is a fresh copy when the backing store is Pebble (its
// Get always returns a copy), so the freezer batch may safely retain it
// across the ModifyAncients call.
func ReadBlockRaw(db ethdb.KeyValueReader, number uint64) []byte {
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	return data
}

// ReadTransactionInfosRaw returns the marshalled `corepb.TransactionRet`
// bytes stored under `tib-<num>` in Pebble, or nil if the row is absent.
// Same fast-path rationale as ReadBlockRaw: avoid round-tripping the proto
// for blocks that will be appended to ancient verbatim.
//
// Slice-1 of the freezer design includes tx-info-per-block in the frozen
// kinds; the per-tx index (`ti-<txid>`) and tx-hash reverse index
// (`tx-<hash>`) remain hot, so they intentionally do not have a *Raw
// counterpart here.
func ReadTransactionInfosRaw(db ethdb.KeyValueReader, number uint64) []byte {
	data, err := db.Get(txInfoBlockKey(number))
	if err != nil {
		return nil
	}
	return data
}

// ReadBlockHashRaw returns the canonical block hash from bytes previously
// loaded by ReadBlockRaw. It scans only BlockHeader.RawData and skips all
// transaction messages without decoding them.
func ReadBlockHashRaw(data []byte) common.Hash {
	hash, err := types.BlockHashFromRaw(data)
	if err != nil {
		return common.Hash{}
	}
	return hash
}

// ReadBlockHashByNumber remains for rare KV-only callers that do not already
// hold the block bytes. It prefers the bounded recent BlockID ring and retains a
// raw-body fallback for databases created before that index existed. The
// freezer runner uses ReadBlockHashRaw on its existing read.
func ReadBlockHashByNumber(db ethdb.KeyValueReader, number uint64) common.Hash {
	hash, _ := ReadBlockHashKV(db, number)
	return hash
}

// ReadBlockStateRootRaw returns the raw 32-byte state root stored under
// `bsr-<hash>`, or nil if absent. Used by the freezer pass to copy the
// row into the `state_roots` ancient table verbatim.
func ReadBlockStateRootRaw(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, err := db.Get(blockStateRootKey(hash.Bytes()))
	if err != nil {
		return nil
	}
	return data
}

// DeleteFrozenBlockRange removes the hot Pebble rows that the slice-3
// freezer has just copied into ancient: `b-<num>` (block proto) and
// `tib-<num>` (tx-info-per-block) for every num in [lo, hi].
//
// Per the slice-1 freezer design, `bh-<hash>`, `bsr-<hash>`, `tx-<hash>`,
// and `ti-<txid>` are intentionally left in Pebble — they are small,
// hash-keyed wallet-hot rows that the freezer does not own. The bounded
// `bnh-<slot>` recent-hash ring also stays hot so the full 256-block TVM
// BLOCKHASH window remains body-free after freezing. A future slice may
// relocate the unbounded hash-keyed rows; until then this helper is narrow.
//
// Implementation: two DeleteRange calls — one per prefix — wrapping the
// half-open `[prefix||lo, prefix||(hi+1))` window. Pebble turns each into
// a range tombstone (O(1) on the write path, compacted away later).
// Memory-backed stores (memorydb, blockbuffer) also implement
// DeleteRange so tests exercise the same code path.
//
// hi is INCLUSIVE: a caller wanting "every block strictly below cutoff"
// passes (lo, cutoff-1). Returns silently when lo > hi (no rows to drop).
func DeleteFrozenBlockRange(db ethdb.KeyValueRangeDeleter, lo, hi uint64) error {
	if lo > hi {
		return nil
	}
	endBlock := hi + 1
	// When hi == MaxUint64 the +1 overflows; that's only reachable via a
	// caller passing the sentinel, which the slice-3 runner never does.
	// Guard anyway so an integration test that pokes the edge cases
	// doesn't trip a panic.
	if endBlock < hi {
		endBlock = hi
	}
	if err := db.DeleteRange(blockKey(lo), blockKey(endBlock)); err != nil {
		return err
	}
	if err := db.DeleteRange(txInfoBlockKey(lo), txInfoBlockKey(endBlock)); err != nil {
		return err
	}
	return nil
}

// BlockRangeBounds returns the prefix-encoded `b-<num>` half-open key
// bounds covering [lo, hi]. Used by the slice-3 freezer runner to call
// Pebble's `Compact(start, limit)` over the range it just deleted so the
// LSM reclaims the freed space promptly.
//
// The bounds are byte-identical to what DeleteFrozenBlockRange would use
// against the `b-` prefix; exposing them separately lets the runner trigger
// compaction without re-deleting (Compact is idempotent — re-running it
// over a fully-compacted range is harmless but wastes IO).
func BlockRangeBounds(lo, hi uint64) (start, limit []byte) {
	endBlock := hi + 1
	if endBlock < hi {
		endBlock = hi
	}
	return blockKey(lo), blockKey(endBlock)
}
