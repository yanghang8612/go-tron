package downloader

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// StagedBodyIterator streams persisted sync-staged body rows in ascending
// block-number order starting at start.
type StagedBodyIterator func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error

// StagedBodyRestoreResult reports the contiguous range recovered from the
// persisted downloader body stage into the in-memory drain buffer.
type StagedBodyRestoreResult struct {
	Restored         int
	TargetHead       uint64
	NextExpected     uint64
	LastRestoredNum  uint64
	LastRestoredHash tcommon.Hash
	HaveLastRestored bool
	NeedPruneTail    bool
	PruneFrom        uint64
	ReadError        error
	InvalidRow       rawdb.SyncStagedBlockRow
	InvalidError     error
}

// StagedBodyRestoreSettlementStepAction names one side effect after restoring
// persisted staged bodies into the local drain buffer.
type StagedBodyRestoreSettlementStepAction uint8

const (
	StagedBodyRestoreSetTargetHead StagedBodyRestoreSettlementStepAction = iota
	StagedBodyRestorePruneStaleTail
)

// StagedBodyRestoreSettlementStep is one downloader-owned restore settlement
// operation. Fields are populated only for actions that need them.
type StagedBodyRestoreSettlementStep struct {
	Action           StagedBodyRestoreSettlementStepAction
	TargetHead       uint64
	PruneFrom        uint64
	LastRestoredNum  uint64
	LastRestoredHash tcommon.Hash
	HaveLastRestored bool
}

// StagedBodyRestoreSettlementPlan decides how a restored staged-body range
// updates session target state and whether a stale persisted tail must be
// pruned.
type StagedBodyRestoreSettlementPlan struct {
	Restore        StagedBodyRestoreResult
	PruneStaleTail bool
	Steps          []StagedBodyRestoreSettlementStep
}

// StagedBodyRestoreSettlementApplier performs the side effects named by a
// restore settlement plan. SyncService owns DB handles and logging; downloader
// owns the stage-recovery decision.
type StagedBodyRestoreSettlementApplier interface {
	SetStagedBodyRestoreTargetHead(targetHead uint64)
	PruneStaleStagedBodyTail(from uint64, lastRestoredNum uint64, lastRestoredHash tcommon.Hash, haveLastRestored bool)
}

// StagedBodyRestoreSettlementApplyResult reports which restore settlement
// steps were dispatched.
type StagedBodyRestoreSettlementApplyResult struct {
	AppliedSteps []StagedBodyRestoreSettlementStepAction
	UnknownSteps []StagedBodyRestoreSettlementStepAction
}

// RestoreStagedBodies restores a contiguous run of persisted downloader body
// rows into buffer, honoring any already-buffered contiguous entries first. A
// gap, path conflict, or read error asks the caller to prune the stale persisted
// tail from PruneFrom; the caller decides whether startup repair is enabled.
func RestoreStagedBodies(start uint64, limit int, targetHead uint64, buffer map[uint64]BufferedBlock, bufferedHashes map[tcommon.Hash]struct{}, path *BlockPath, iterate StagedBodyIterator) StagedBodyRestoreResult {
	result := StagedBodyRestoreResult{
		TargetHead:   targetHead,
		NextExpected: start,
	}
	if limit <= 0 || buffer == nil {
		return result
	}
	expected := start
	markRestored := func(num uint64, hash tcommon.Hash) {
		if num > result.TargetHead {
			result.TargetHead = num
		}
		result.LastRestoredNum = num
		result.LastRestoredHash = hash
		result.HaveLastRestored = true
		result.Restored++
		expected++
		result.NextExpected = expected
	}
	requestPrune := func(from uint64) {
		if !result.NeedPruneTail {
			result.NeedPruneTail = true
			result.PruneFrom = from
		}
		result.NextExpected = from
	}
	consumeBuffered := func() bool {
		for result.Restored < limit {
			buffered, ok := buffer[expected]
			if !ok {
				result.NextExpected = expected
				return true
			}
			markRestored(buffered.Num, buffered.Hash)
		}
		return false
	}
	if !consumeBuffered() || iterate == nil {
		return result
	}
	err := iterate(start, func(row rawdb.SyncStagedBlockRow) (bool, error) {
		for row.Number > expected {
			if !consumeBuffered() {
				return false, nil
			}
			if row.Number > expected {
				requestPrune(expected)
				return false, nil
			}
		}
		if row.Number < expected {
			return true, nil
		}
		block, err := types.UnmarshalBlock(row.Raw)
		if err != nil {
			result.InvalidRow = row
			result.InvalidError = err
			requestPrune(row.Number)
			return false, nil
		}
		if err := ValidateBufferedBlockMetadata(BufferedBlock{Raw: row.Raw, Hash: row.Hash, Num: row.Number}, block); err != nil {
			result.InvalidRow = row
			result.InvalidError = err
			requestPrune(row.Number)
			return false, nil
		}
		bid := types.BlockID{Hash: row.Hash, Num: row.Number}
		if path != nil {
			nextPath, ok := (*path).Reserve(bid)
			if !ok {
				requestPrune(row.Number)
				return false, nil
			}
			*path = nextPath
		}
		buffer[row.Number] = BufferedBlock{
			Raw:  row.Raw,
			Hash: row.Hash,
			Num:  row.Number,
		}
		if bufferedHashes != nil {
			bufferedHashes[bid.Hash] = struct{}{}
		}
		markRestored(row.Number, row.Hash)
		return result.Restored < limit, nil
	})
	if err != nil {
		result.ReadError = err
		requestPrune(expected)
		return result
	}
	if result.Restored < limit && consumeBuffered() {
		requestPrune(expected)
	}
	return result
}

// PlanStagedBodyRestoreSettlement returns the restore-settlement side effects
// for one staged-body restore result.
func PlanStagedBodyRestoreSettlement(restore StagedBodyRestoreResult, pruneStaleTail bool) StagedBodyRestoreSettlementPlan {
	plan := StagedBodyRestoreSettlementPlan{
		Restore:        restore,
		PruneStaleTail: pruneStaleTail,
		Steps: []StagedBodyRestoreSettlementStep{
			{Action: StagedBodyRestoreSetTargetHead, TargetHead: restore.TargetHead},
		},
	}
	if pruneStaleTail && restore.NeedPruneTail {
		plan.Steps = append(plan.Steps, StagedBodyRestoreSettlementStep{
			Action:           StagedBodyRestorePruneStaleTail,
			PruneFrom:        restore.PruneFrom,
			LastRestoredNum:  restore.LastRestoredNum,
			LastRestoredHash: restore.LastRestoredHash,
			HaveLastRestored: restore.HaveLastRestored,
		})
	}
	return plan
}

// ApplyStagedBodyRestoreSettlementPlan executes downloader-owned restore
// settlement decisions against the caller's persistence/runtime adapter.
func ApplyStagedBodyRestoreSettlementPlan(plan StagedBodyRestoreSettlementPlan, applier StagedBodyRestoreSettlementApplier) StagedBodyRestoreSettlementApplyResult {
	var result StagedBodyRestoreSettlementApplyResult
	if applier == nil {
		return result
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case StagedBodyRestoreSetTargetHead:
			applier.SetStagedBodyRestoreTargetHead(step.TargetHead)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case StagedBodyRestorePruneStaleTail:
			applier.PruneStaleStagedBodyTail(step.PruneFrom, step.LastRestoredNum, step.LastRestoredHash, step.HaveLastRestored)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}
