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
	return WriteBlockEncoded(db, block, data)
}

// WriteBlockEncoded writes a block using an already-marshaled protobuf
// payload. The caller keeps data immutable after the call so layered stores
// can retain the bytes without another full-block copy.
func WriteBlockEncoded(db ethdb.KeyValueWriter, block *types.Block, data []byte) error {
	if db == nil || block == nil {
		return errors.New("write encoded block: nil database or block")
	}
	number := block.Number()
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], number)
	var err error
	if writer, ok := db.(keyPartsOwnedValueWriter); ok {
		err = writer.PutKeyPartsOwnedValue(blockPrefix, num[:], data)
	} else if writer, ok := db.(ownedValueWriter); ok {
		err = writer.PutOwnedValue(blockKey(number), data)
	} else {
		err = db.Put(blockKey(number), data)
	}
	if err != nil {
		return err
	}
	return WriteBlockIndexes(db, block)
}

// WriteBlockIndexes writes the immutable hash-to-number lookup and bounded
// recent number-to-hash ring without rewriting the block body.
func WriteBlockIndexes(db ethdb.KeyValueWriter, block *types.Block) error {
	if db == nil || block == nil {
		return errors.New("write block indexes: nil database or block")
	}
	number := block.Number()
	hash := block.Hash()
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], number)
	if writer, ok := db.(keyPartsWriter); ok {
		if err := writer.PutKeyParts(blockHashPrefix, hash[:], num[:]); err != nil {
			return err
		}
	} else if err := db.Put(blockHashKey(hash[:]), num[:]); err != nil {
		return err
	}
	var slot [8]byte
	binary.BigEndian.PutUint64(slot[:], number%blockNumberHashSlots)
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.PutKeyParts(blockNumberHashPrefix, slot[:], hash[:])
	}
	return db.Put(blockNumberHashKey(number), hash[:])
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
	return readBlock(db, number, false)
}

// ReadBlockReusable transfers an owned storage buffer to the decoded block so
// a subsequent MarshalReusable can avoid a second full-block allocation.
func ReadBlockReusable(db *ChainDB, number uint64) *types.Block {
	return readBlock(db, number, true)
}

// ReadStoredBlockForReplay is the fresh-format replay reader. The retired V2
// freezer loaned mmap-backed fields here; the current format deliberately uses
// the owned reusable path instead.
func ReadStoredBlockForReplay(db *ChainDB, number uint64) *types.Block {
	return readBlock(db, number, true)
}

func readBlock(db *ChainDB, number uint64, reusable bool) *types.Block {
	if data, ok := readAncient(db, ancientBlocks, number); ok {
		block, err := unmarshalStoredBlock(data, reusable)
		if err != nil {
			return nil
		}
		return block
	}
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	block, err := unmarshalStoredBlock(data, reusable)
	if err != nil {
		return nil
	}
	return block
}

func unmarshalStoredBlock(data []byte, reusable bool) (*types.Block, error) {
	if reusable {
		return types.UnmarshalBlockOwned(data)
	}
	return types.UnmarshalBlock(data)
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

// ReadBlockHash returns the canonical BlockID at number without fully decoding
// the block. Fresh databases answer recent requests from the bounded ring and
// fall back to the freezer/hot canonical body for older heights.
func ReadBlockHash(db *ChainDB, number uint64) (common.Hash, bool) {
	if hash, ok := readBlockHashIndex(db, number); ok {
		return hash, true
	}
	if data, ok := readAncient(db, ancientBlocks, number); ok {
		hash, err := types.BlockHashFromRaw(data)
		if err == nil {
			return hash, true
		}
	}
	return readBlockHashRawKV(db, number)
}

// ReadBlockHashKV is the hot/layered-store variant of ReadBlockHash.
func ReadBlockHashKV(db ethdb.KeyValueReader, number uint64) (common.Hash, bool) {
	if hash, ok := readBlockHashIndex(db, number); ok {
		return hash, true
	}
	return readBlockHashRawKV(db, number)
}

func readBlockHashIndex(db ethdb.KeyValueReader, number uint64) (common.Hash, bool) {
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], number%blockNumberHashSlots)
	var (
		data []byte
		err  error
	)
	if reader, ok := db.(cachedNoCopyKeyPartsReader); ok {
		data, err = reader.GetNoCopyCachedKeyParts(blockNumberHashPrefix, suffix[:])
	} else {
		data, err = readStateNoCopyCached(db, blockNumberHashKey(number))
	}
	if err != nil || len(data) != common.HashLength || binary.BigEndian.Uint64(data[:8]) != number {
		return common.Hash{}, false
	}
	return common.BytesToHash(data), true
}

func readBlockHashRawKV(db ethdb.KeyValueReader, number uint64) (common.Hash, bool) {
	data, err := readBlockRawNoCopy(db, number)
	if err != nil {
		return common.Hash{}, false
	}
	hash, err := types.BlockHashFromRaw(data)
	if err != nil {
		return common.Hash{}, false
	}
	return hash, true
}

func readBlockRawNoCopy(db ethdb.KeyValueReader, number uint64) ([]byte, error) {
	key := blockKey(number)
	if reader, ok := db.(noCopyKeyValueReader); ok {
		return reader.GetNoCopy(key)
	}
	return db.Get(key)
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
