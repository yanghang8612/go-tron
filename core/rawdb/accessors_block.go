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
	if err := db.Put(blockKey(block.Number()), data); err != nil {
		return err
	}
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, block.Number())
	return db.Put(blockHashKey(block.Hash().Bytes()), num)
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

// ReadBlockKV is the KV-only variant of ReadBlock, for callers that hold a
// plain `ethdb.KeyValueReader` and know the block they want is hot (i.e.
// above the freezer cutoff). The VM's BLOCKHASH opcode is the canonical
// user: its 256-block lookback window sits above the 128-block freezer
// margin, so reads here are guaranteed to land in Pebble.
//
// Slice-2's audit doc records this as a "intentionally KV-only" call
// site; if the freezer margin is ever shrunk below 256 this site needs
// revisiting.
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
