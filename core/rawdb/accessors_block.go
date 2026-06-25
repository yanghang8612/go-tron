package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// ancientBlocks names the freezer table holding marshalled `corepb.Block`
// blobs keyed by block number. gtron's block proto is monolithic (header +
// transaction list in a single message), so unlike geth we don't split
// "headers" and "bodies" into separate ancient tables.
const ancientBlocks = AncientBlocksTable

func WriteBlock(db ethdb.KeyValueWriter, block *types.Block) error {
	data, err := block.Marshal()
	if err != nil {
		return err
	}
	if err := db.Put(blockKey(block.Number()), data); err != nil {
		return err
	}
	return WriteBlockNumber(db, block.Hash(), block.Number())
}

func WriteBlockNumber(db ethdb.KeyValueWriter, hash common.Hash, number uint64) error {
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, number)
	return db.Put(blockHashKey(hash.Bytes()), num)
}

func DeleteBlockNumber(db ethdb.KeyValueWriter, hash common.Hash) error {
	return db.Delete(blockHashKey(hash.Bytes()))
}

// ReadBlock returns the block at the given number, consulting the freezer
// first for any block below the ancient cutoff and falling back to the
// hot KV store otherwise. Returns nil if the block is unknown in both
// stores (or fails to decode).
//
// The two-tier read order is the standard freezer pattern: ancient is
// append-only and never holds a row that hasn't been flushed to disk, so
// hitting it first for in-range numbers avoids paying a Pebble Get for
// frozen blocks (the common case once the freezer has caught up).
func ReadBlock(db *ChainDB, number uint64) *types.Block {
	if data, ok := readAncient(db, ancientBlocks, number); ok {
		block, err := types.UnmarshalBlock(data)
		if err != nil {
			return nil
		}
		return block
	}
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil
	}
	return block
}

// ReadBlockStrict returns the block at the given number and reports malformed
// hot/freezer rows instead of folding them into "missing". Legacy ReadBlock
// keeps its nil-on-error contract for chain code that treats corrupt rows the
// same way old hot-only accessors did.
func ReadBlockStrict(db *ChainDB, number uint64) (*types.Block, bool, error) {
	data, ok, err := ReadBlockRawStrict(db, number)
	if err != nil || !ok {
		return nil, ok, err
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil, true, fmt.Errorf("rawdb: block %d decode: %w", number, err)
	}
	if block.Number() != number {
		return block, true, fmt.Errorf("rawdb: block row %d contains block number %d", number, block.Number())
	}
	return block, true, nil
}

// ReadBlockNumber returns the block number persisted for the given block hash,
// or nil if unknown. The hot `bh-<hash>` row is preferred; on a miss, a ChainDB
// with an attached cold chain-index sidecar can resolve historical hashes
// without requiring every old lookup row to stay in Pebble.
func ReadBlockNumber(db *ChainDB, hash common.Hash) *uint64 {
	num, ok, err := ReadBlockNumberStrict(db, hash)
	if err != nil || !ok {
		return nil
	}
	return &num
}

// ReadBlockNumberStrict retrieves the block number for a block hash and
// surfaces malformed hot rows or cold sidecar lookup errors.
func ReadBlockNumberStrict(db *ChainDB, hash common.Hash) (uint64, bool, error) {
	if db == nil {
		return 0, false, fmt.Errorf("rawdb: nil database during read block number")
	}
	key := blockHashKey(hash.Bytes())
	exists, err := db.Has(key)
	if err != nil {
		return 0, false, err
	}
	if exists {
		data, err := db.Get(key)
		if err != nil {
			return 0, false, err
		}
		if len(data) != 8 {
			return 0, true, fmt.Errorf("rawdb: block number lookup %x has length %d, want 8", hash.Bytes(), len(data))
		}
		return binary.BigEndian.Uint64(data), true, nil
	}
	if db.chainIndex != nil {
		num, ok, err := db.chainIndex.BlockNumberByHash(hash)
		if err != nil || !ok {
			return 0, ok, err
		}
		return num, true, nil
	}
	return 0, false, nil
}

// readAncient is the per-accessor freezer probe. Returns (data, true) when
// the table reports an in-range entry for `number`; returns (_, false) on
// any "not in ancient" / out-of-bounds / unknown-table outcome so the
// caller can fall through to Pebble. Surfacing other freezer errors as a
// silent miss matches the existing accessor contract (broken decode
// returns nil rather than panicking).
func readAncient(db *ChainDB, kind string, number uint64) ([]byte, bool) {
	if db == nil || db.AncientReader == nil {
		return nil, false
	}
	data, err := db.Ancient(kind, number)
	if err != nil {
		if errors.Is(err, ErrNotInAncient) {
			return nil, false
		}
		// Any other error (filesystem trouble) also degrades gracefully to
		// the KV path; loud failure isn't useful here because the next pass
		// will simply retry against the same broken file.
		return nil, false
	}
	return data, true
}

func readAncientStrict(db *ChainDB, kind string, number uint64) ([]byte, bool, error) {
	if db == nil || db.AncientReader == nil {
		return nil, false, nil
	}
	data, err := db.Ancient(kind, number)
	if err != nil {
		if errors.Is(err, ErrNotInAncient) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return data, true, nil
}

// BlockHashReader is an optional capability interface for the KV store the
// VM holds (TVM.DB). When the store implements it, BLOCKHASH and the
// genesis-hash read behind CHAINID resolve block hashes through it instead
// of a raw blockKey row. The chain injects an implementation whose lookup
// falls through to the ancient store: the slice-3 freezer deletes hot
// b-<num> rows past (solidified - margin), and with the default 128-block
// margin that line sits INSIDE the opcode's 256-block lookback window —
// a bare KV read goes blind for the older part of the window (and for
// genesis once block 0 is frozen).
type BlockHashReader interface {
	// BlockHashByNumber returns the block hash at the given height and
	// whether it could be resolved at all.
	BlockHashByNumber(number uint64) (common.Hash, bool)
}

// BlockHashReaderStrict is the error-returning variant used by execution and
// verification paths that must distinguish a genuinely missing canonical block
// from a corrupt or unreadable hot/freezer block row.
type BlockHashReaderStrict interface {
	BlockHashByNumberStrict(number uint64) (common.Hash, bool, error)
}

// ReadBlockKV is the KV-only variant of ReadBlock, for callers that hold a
// plain `ethdb.KeyValueReader`. NOTE: hot b-<num> rows are deleted by the
// slice-3 freezer once a block is frozen (default margin: 128 blocks below
// solidified), so this CANNOT serve the full 256-block BLOCKHASH window —
// production VM paths must hand the TVM a store implementing
// BlockHashReader instead; this read remains as the fallback for tests
// that seed a bare memdb. (The Nile 16,745,722 JustLink VRF stall came
// from relying on this read alone.)
func ReadBlockKV(db ethdb.KeyValueReader, number uint64) *types.Block {
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil
	}
	return block
}
