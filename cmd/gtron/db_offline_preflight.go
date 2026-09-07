package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// offlineChainBoundary is read without constructing a mutable chain or node.
// Missing derived pointers must not silently become zero-valued defaults.
type offlineChainBoundary struct {
	HeadBlock       uint64      `json:"head_block"`
	HeadHash        common.Hash `json:"head_hash"`
	SolidifiedBlock uint64      `json:"solidified_block"`
	SolidifiedHash  common.Hash `json:"solidified_hash"`
}

func readOfflineChainBoundary(db ethdb.KeyValueReader) (offlineChainBoundary, error) {
	var out offlineChainBoundary
	head, ok, err := rawdb.ReadHeadBlockHashStrict(db)
	if err != nil {
		return out, err
	}
	if !ok || head == (common.Hash{}) {
		return out, fmt.Errorf("offline maintenance requires a persisted nonzero head")
	}
	readNumber := func(name string) (uint64, error) {
		value, ok, err := rawdb.ReadDynamicPropertyStrict(db, name)
		if err != nil {
			return 0, err
		}
		if !ok || len(value) != 8 {
			return 0, fmt.Errorf("offline maintenance requires an 8-byte %s", name)
		}
		n := binary.BigEndian.Uint64(value)
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("offline maintenance: negative %s", name)
		}
		return n, nil
	}
	number, err := readNumber("latest_block_header_number")
	if err != nil {
		return out, err
	}
	if number != binary.BigEndian.Uint64(head[:8]) {
		return out, fmt.Errorf("offline maintenance: head height does not match dynamic properties")
	}
	value, ok, err := rawdb.ReadDynamicPropertyStrict(db, "latest_block_header_hash")
	if err != nil {
		return out, err
	}
	if !ok || !bytes.Equal(value, head[:]) {
		return out, fmt.Errorf("offline maintenance: dynamic head hash mismatch")
	}
	canonical, ok, err := rawdb.ReadBlockHashByNumberStrict(db, number)
	if err != nil {
		return out, err
	}
	if !ok || canonical != head {
		return out, fmt.Errorf("offline maintenance: canonical head mismatch at %d", number)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageExecution, rawdb.StageFinish} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			return out, err
		}
		if !ok || !row.HasBlockHash || row.BlockNum != number || row.BlockHash != head {
			return out, fmt.Errorf("offline maintenance: %s must match the persisted head", stage)
		}
	}
	solid, err := readNumber("latest_solidified_block_num")
	if err != nil {
		return out, err
	}
	if solid == 0 || solid > number {
		return out, fmt.Errorf("offline maintenance: solidified block %d is outside [1,%d]", solid, number)
	}
	solidHash, ok, err := rawdb.ReadBlockHashByNumberStrict(db, solid)
	if err != nil {
		return out, err
	}
	if !ok || solidHash == (common.Hash{}) {
		return out, fmt.Errorf("offline maintenance: missing canonical solidified block %d", solid)
	}
	return offlineChainBoundary{number, head, solid, solidHash}, nil
}

func offlineSpaceAvailable(path string, floor, budget uint64) (uint64, error) {
	usage, err := vfs.Default.GetDiskUsage(path)
	if err != nil {
		return 0, fmt.Errorf("inspect free space at %s: %w", path, err)
	}
	if budget > math.MaxUint64-floor || usage.AvailBytes < floor+budget {
		return usage.AvailBytes, fmt.Errorf("offline maintenance: free space %d at %s cannot cover reserve %d plus work budget %d", usage.AvailBytes, path, floor, budget)
	}
	return usage.AvailBytes, nil
}

// watchOfflineSpace requests cancellation between builder writes. Admission
// must already reserve the complete conservative batch budget; this watcher
// handles competing services' allocations and is not a hard write limiter.
func watchOfflineSpace(parent context.Context, paths []string, floor uint64) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, path := range paths {
					if _, err := offlineSpaceAvailable(path, floor, 0); err != nil {
						cancel(err)
						return
					}
				}
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled); <-done }
}
