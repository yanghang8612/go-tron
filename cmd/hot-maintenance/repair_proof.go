package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// repairTargetSlice is an exact block-aligned TARGET interval. A partially
// bound bucket may have a COLD prefix, but none of that prefix may be copied
// from TARGET: its physical target rows were already released.
type repairTargetSlice struct {
	Bucket    uint64 `json:"bucket"`
	FromBlock uint64 `json:"from_block"`
	ToBlock   uint64 `json:"to_block"`
	FromTxNum uint64 `json:"from_tx_num"`
	ToTxNum   uint64 `json:"to_tx_num"`
}

func selectRepairTargetSlices(ctx context.Context, p retirePlan, manager *rawdb.HistoryStagingManager, chain *rawdb.ChainDB, fromTx, toTx uint64) ([]repairTargetSlice, error) {
	if ctx == nil || manager == nil || chain == nil || fromTx == 0 || toTx < fromTx {
		return nil, errors.New("repair: invalid TARGET interval or canonical reader")
	}
	var slices []repairTargetSlice
	nextTx := fromTx
	covered := false
	for _, row := range p.Buckets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		blocks, err := retireCanonicalBlocks(ctx, manager, chain, row.Bucket)
		if err != nil {
			return nil, err
		}
		var current *repairTargetSlice
		for _, block := range blocks {
			if block.EndTxNum < fromTx || block.BeginTxNum > toTx {
				continue
			}
			if row.Route.Owner != rawdb.HistoryStagingOwnerTarget || !row.Route.SourceCleared || row.Route.TargetCleared || block.BeginTxNum != nextTx || block.EndTxNum > toTx {
				return nil, fmt.Errorf("repair: block %d is not an exact, continuous TARGET source", block.Number)
			}
			if row.OldBinding != nil {
				for _, span := range row.OldBinding.Spans {
					if span.From <= block.Number && block.Number <= span.To {
						return nil, fmt.Errorf("repair: block %d is already COLD-owned", block.Number)
					}
				}
			}
			if current == nil {
				current = &repairTargetSlice{Bucket: row.Bucket, FromBlock: block.Number, ToBlock: block.Number, FromTxNum: block.BeginTxNum, ToTxNum: block.EndTxNum}
			} else {
				current.ToBlock, current.ToTxNum = block.Number, block.EndTxNum
			}
			covered = true
			if block.EndTxNum == ^uint64(0) {
				if block.EndTxNum != toTx {
					return nil, errors.New("repair: TARGET tx range overflows")
				}
			} else {
				nextTx = block.EndTxNum + 1
			}
		}
		if current != nil {
			slices = append(slices, *current)
		}
	}
	if !covered || (toTx != ^uint64(0) && nextTx != toTx+1) {
		return nil, errors.New("repair: requested TARGET interval is not fully covered")
	}
	return slices, nil
}

// validateRepairTargetSlices rederives every journal slice from current,
// strictly canonical block/tx ranges. A resumed bucket may already have the
// newly certified cold span, so this proof deliberately does not reject an
// overlap with the current binding; the caller separately verifies that such
// a binding is exactly the frozen candidate and was durably certified.
func validateRepairTargetSlices(ctx context.Context, slices []repairTargetSlice, p retirePlan, manager *rawdb.HistoryStagingManager, chain *rawdb.ChainDB, fromTx, toTx uint64) error {
	if len(slices) == 0 || len(slices) != len(p.Buckets) || toTx < fromTx {
		return errors.New("repair: journal TARGET slices do not cover requested buckets")
	}
	nextTx := fromTx
	for i, item := range slices {
		row := p.Buckets[i]
		if item.Bucket != row.Bucket || row.Route.Owner != rawdb.HistoryStagingOwnerTarget || !row.Route.SourceCleared || row.Route.TargetCleared {
			return fmt.Errorf("repair: journal bucket %d is not retained TARGET", item.Bucket)
		}
		blocks, err := retireCanonicalBlocks(ctx, manager, chain, item.Bucket)
		if err != nil {
			return err
		}
		first := item.Bucket * rawdb.StateHistoryChunkBucketBlocks
		if item.FromBlock < first || item.ToBlock < item.FromBlock || item.ToBlock >= first+uint64(len(blocks)) || item.FromTxNum != nextTx {
			return fmt.Errorf("repair: journal bucket %d block/tx bounds differ", item.Bucket)
		}
		for n := item.FromBlock; n <= item.ToBlock; n++ {
			block := blocks[n-first]
			if block.BeginTxNum != nextTx || block.EndTxNum < nextTx || block.EndTxNum > toTx {
				return fmt.Errorf("repair: journal block %d is not an exact canonical tx interval", n)
			}
			if block.EndTxNum == ^uint64(0) {
				if block.EndTxNum != toTx || n != item.ToBlock {
					return errors.New("repair: journal TARGET tx range overflows")
				}
			} else {
				nextTx = block.EndTxNum + 1
			}
		}
		if blocks[item.FromBlock-first].BeginTxNum != item.FromTxNum || blocks[item.ToBlock-first].EndTxNum != item.ToTxNum {
			return fmt.Errorf("repair: journal bucket %d slice tx endpoints changed", item.Bucket)
		}
	}
	if toTx != ^uint64(0) && nextTx != toTx+1 {
		return errors.New("repair: journal TARGET slices do not reach requested end")
	}
	return nil
}
