// Freezer-side helpers used by the slice-3 background freezing goroutine.
//
// The runner package (core/freezer) needs to (a) read the raw KV bytes for
// every freezable kind at a given block number and (b) delete those rows
// from Pebble once they have been durably appended to ancient. Both
// concerns straddle the rawdb key-layout boundary, so the helpers live
// here next to the schema rather than reaching into private prefixes from
// outside the package.
//
// The freezer runner owns only the num-keyed hot rows (`b-<num>`,
// `tib-<num>`). Hash-keyed lookup rows (`bh-<hash>`, `tx-<hash>`,
// `ti-<txid>`, `bsr-<hash>`) are pruned later only when a verified cold
// chain-index sidecar covers the same chain-freezer range.

package rawdb

import (
	"errors"
	"fmt"

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
	if cdb, ok := db.(*ChainDB); ok {
		if data, ok := readAncient(cdb, ancientBlocks, number); ok {
			return data
		}
	}
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	return data
}

// ReadBlockRawStrict returns the raw block row at number and preserves freezer
// or hot-KV read errors. It does not decode the block; callers that need to
// validate the proto and key number should use ReadBlockStrict.
func ReadBlockRawStrict(db ethdb.KeyValueReader, number uint64) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read block raw")
	}
	if cdb, ok := db.(*ChainDB); ok {
		if cdb == nil {
			return nil, false, fmt.Errorf("rawdb: nil database during read block raw")
		}
		data, ok, err := readAncientStrict(cdb, ancientBlocks, number)
		if err != nil || ok {
			return data, ok, err
		}
	}
	key := blockKey(number)
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	data, err := db.Get(key)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
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
	if cdb, ok := db.(*ChainDB); ok {
		if data, ok := readAncient(cdb, ancientTxInfos, number); ok {
			return data
		}
	}
	data, err := db.Get(txInfoBlockKey(number))
	if err != nil {
		return nil
	}
	return data
}

// WriteTransactionInfosRaw stores a pre-marshalled `corepb.TransactionRet`
// blob under `tib-<num>` without decoding or validating it. Normal block
// execution and backfill paths must use WriteTransactionInfosByBlock instead;
// this helper exists for raw snapshot/freezer replay and corruption fixtures
// that intentionally need to preserve bytes at the schema boundary.
func WriteTransactionInfosRaw(db ethdb.KeyValueWriter, number uint64, data []byte) error {
	return db.Put(txInfoBlockKey(number), data)
}

// ReadBlockHashByNumber returns the canonical block hash for the given block
// number. When the caller passes a ChainDB, this walks the normal ReadBlock
// path, so frozen block bodies are served from ancient and hot bodies from KV.
// Plain KV readers keep the original hot-only path.
//
// The freezer pass uses this to resolve the `bsr-<hash>` key for each
// block in the freeze range — `bsr-<hash>` is the hash-keyed state-root
// row that the freezer copies into the num-keyed `state_roots` ancient
// table. Once the freezer has caught up the row also gets deleted from
// Pebble.
//
// Cost: one Pebble Get + one proto Unmarshal + Hash() per call. Hot enough
// for a per-block freezer pass; not intended for VM/RPC hot paths.
func ReadBlockHashByNumber(db ethdb.KeyValueReader, number uint64) common.Hash {
	if cdb, ok := db.(*ChainDB); ok {
		block := ReadBlock(cdb, number)
		if block == nil {
			return common.Hash{}
		}
		return block.Hash()
	}
	data, err := db.Get(blockKey(number))
	if err != nil {
		return common.Hash{}
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return common.Hash{}
	}
	return block.Hash()
}

// ReadBlockStateRootRaw returns the raw 32-byte state root stored under
// `bsr-<hash>`, or nil if absent. Used by the freezer pass to copy the
// row into the `state_roots` ancient table verbatim.
func ReadBlockStateRootRaw(db ethdb.KeyValueReader, hash common.Hash) []byte {
	data, err := db.Get(blockStateRootKey(hash.Bytes()))
	if err == nil && len(data) == common.HashLength {
		return data
	}
	if cdb, ok := db.(*ChainDB); ok {
		numPtr := ReadBlockNumber(cdb, hash)
		if numPtr == nil {
			return nil
		}
		if data, ok := readAncient(cdb, ancientStateRoots, *numPtr); ok && len(data) == common.HashLength {
			return data
		}
	}
	return nil
}

// ReadBlockStateRootRawStrict returns the raw 32-byte state root for hash and
// reports whether a state-root row was found. Unlike ReadBlockStateRootRaw, it
// surfaces malformed hot rows, malformed ancient rows, and cold chain-index
// lookup errors instead of folding them into "missing".
func ReadBlockStateRootRawStrict(db ethdb.KeyValueReader, hash common.Hash) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read block state root")
	}
	if cdb, ok := db.(*ChainDB); ok && cdb == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read block state root")
	}
	key := blockStateRootKey(hash.Bytes())
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if exists {
		data, err := db.Get(key)
		if err != nil {
			return nil, false, err
		}
		if len(data) != common.HashLength {
			return nil, true, fmt.Errorf("rawdb: block state root %x has length %d, want %d", hash.Bytes(), len(data), common.HashLength)
		}
		return data, true, nil
	}

	cdb, ok := db.(*ChainDB)
	if !ok {
		return nil, false, nil
	}
	num, ok, err := ReadBlockNumberStrict(cdb, hash)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return readAncientBlockStateRootRawStrict(cdb, hash, num)
}

func readAncientBlockStateRootRawStrict(db *ChainDB, hash common.Hash, number uint64) ([]byte, bool, error) {
	if db == nil || db.AncientReader == nil {
		return nil, false, nil
	}
	exists, err := db.HasAncient(ancientStateRoots, number)
	if err != nil {
		if errors.Is(err, ErrNotInAncient) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	data, err := db.Ancient(ancientStateRoots, number)
	if err != nil {
		if errors.Is(err, ErrNotInAncient) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(data) != common.HashLength {
		return nil, true, fmt.Errorf("rawdb: ancient block state root %x at block %d has length %d, want %d", hash.Bytes(), number, len(data), common.HashLength)
	}
	return data, true, nil
}

// DeleteFrozenBlockRange removes the hot Pebble rows that the slice-3
// freezer has just copied into ancient: `b-<num>` (block proto) and
// `tib-<num>` (tx-info-per-block) for every num in [lo, hi].
//
// `bh-<hash>`, `bsr-<hash>`, `tx-<hash>`, and `ti-<txid>` are intentionally
// left to the chain-lookup prune lifecycle because deleting them safely
// requires a verified chain-index sidecar. This helper stays narrow so the
// freezer writer only deletes rows it copied in the same pass.
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
