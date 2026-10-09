package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// strictBoundary cross-checks the recent hot head with dynamic properties,
// canonical hashes, and durable stage pointers before offline compaction. If
// a canonical anchor is ancient-only, it fails closed; archive reads require
// a separate ancient-aware ChainDB path. An absent canonical anchor never
// verifies; the optional stored solid hash may be absent.
func strictBoundary(db ethdb.KeyValueReader) (chainBoundary, error) {
	var out chainBoundary
	head, ok, err := rawdb.ReadHeadBlockHashStrict(db)
	if err != nil {
		return out, err
	}
	if !ok || head == (common.Hash{}) {
		return out, errors.New("compact requires a nonzero persisted head")
	}
	readNumber := func(name string) (uint64, error) {
		b, ok, err := rawdb.ReadDynamicPropertyStrict(db, name)
		if err != nil {
			return 0, err
		}
		if !ok || len(b) != 8 {
			return 0, fmt.Errorf("compact requires 8-byte dynamic property %q", name)
		}
		n := binary.BigEndian.Uint64(b)
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("negative dynamic property %q", name)
		}
		return n, nil
	}
	number, err := readNumber("latest_block_header_number")
	if err != nil {
		return out, err
	}
	if number != binary.BigEndian.Uint64(head[:8]) {
		return out, errors.New("dynamic head height disagrees with persisted head hash")
	}
	dynamicHash, ok, err := rawdb.ReadDynamicPropertyStrict(db, "latest_block_header_hash")
	if err != nil {
		return out, err
	}
	if !ok || !bytes.Equal(dynamicHash, head[:]) {
		return out, errors.New("dynamic head hash disagrees with persisted head")
	}
	canonical, ok, err := rawdb.ReadBlockHashByNumberStrict(db, number)
	if err != nil {
		return out, err
	}
	if !ok || canonical != head {
		return out, errors.New("canonical head hash disagrees with persisted head")
	}
	for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			return out, err
		}
		if !ok || !row.HasBlockHash || row.BlockNum != number || row.BlockHash != head {
			return out, fmt.Errorf("%s does not match canonical head", stage)
		}
	}
	for _, stage := range []rawdb.StageID{rawdb.StageStateHistoryIndex, rawdb.StageCommitment} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			return out, err
		}
		if !ok || !row.HasBlockHash || row.BlockNum > number {
			return out, fmt.Errorf("%s pointer absent or beyond head", stage)
		}
		hash, found, err := rawdb.ReadBlockHashByNumberStrict(db, row.BlockNum)
		if err != nil {
			return out, err
		}
		if !found || hash != row.BlockHash {
			return out, fmt.Errorf("%s pointer disagrees with canonical hash", stage)
		}
	}
	solid, err := readNumber("latest_solidified_block_num")
	if err != nil {
		return out, err
	}
	if solid == 0 || solid > number {
		return out, errors.New("solid height outside canonical head")
	}
	solidHash, ok, err := rawdb.ReadBlockHashByNumberStrict(db, solid)
	if err != nil {
		return out, err
	}
	if !ok || solidHash == (common.Hash{}) {
		return out, errors.New("canonical solid hash missing")
	}
	storedSolid, present, err := rawdb.ReadHeadSolidBlockHashStrict(db)
	if err != nil {
		return out, err
	}
	if present && storedSolid != solidHash {
		return out, errors.New("stored solid hash disagrees with canonical solid hash")
	}
	return chainBoundary{number, head.Hex(), solid, solidHash.Hex()}, nil
}
