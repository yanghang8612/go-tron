package rawdb

import (
	"encoding/binary"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// ancientBlocks names the freezer table holding marshalled `corepb.Block`
// blobs keyed by block number. gtron's block proto is monolithic (header +
// transaction list in a single message), so unlike geth we don't split
// "headers" and "bodies" into separate ancient tables.
const ancientBlocks = "bodies"

func WriteBlock(db ethdb.KeyValueWriter, block *types.Block) error {
	data, err := block.Marshal()
	if err != nil {
		return err
	}
	number := block.Number()
	hash := block.Hash()
	if err := db.Put(blockKey(number), data); err != nil {
		return err
	}
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], number)
	if err := db.Put(blockHashKey(hash[:]), num[:]); err != nil {
		return err
	}
	var slot [8]byte
	binary.BigEndian.PutUint64(slot[:], number%blockNumberHashSlots)
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.PutKeyParts(blockNumberHashPrefix, slot[:], hash[:])
	}
	return db.Put(blockNumberHashKey(number), hash[:])
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

// ReadBlockNumber returns the block number persisted for the given block
// hash, or nil if unknown. Slice 1 of the freezer design keeps `bh-<hash>`
// hot, so this accessor is KV-only — the `*ChainDB` parameter exists for
// signature uniformity with other chain readers.
func ReadBlockNumber(db *ChainDB, hash common.Hash) *uint64 {
	data, err := db.Get(blockHashKey(hash.Bytes()))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}

// ReadBlockHash returns the canonical BlockID hash at number without decoding
// the block. New databases answer from the bounded recent BlockID ring. For
// databases created before that index existed, the fallback scans the raw hot
// or ancient block protobuf and extracts only BlockHeader.RawData.
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

// ReadBlockHashKV is the hot-store variant of ReadBlockHash. The raw-body
// fallback keeps pre-index databases compatible and uses immutable no-copy
// overlay values when the reader advertises that optional capability.
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
