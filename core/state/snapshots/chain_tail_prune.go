package snapshots

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	chainFreezerTailPruneReasonReady                = "ready"
	chainFreezerTailPruneReasonMissingFreezerStage  = "missing chain freezer stage"
	chainFreezerTailPruneReasonMissingLookupStage   = "missing chain lookup prune stage"
	chainFreezerTailPruneReasonMissingEventLogStage = "missing event-log build stage"
	chainFreezerTailPruneReasonZeroRetainBlocks     = "zero retain blocks"
	chainFreezerTailPruneReasonEmptyAncientStore    = "empty ancient store"
	chainFreezerTailPruneReasonTailAboveAncientHead = "current tail above ancient head"
	chainFreezerTailPruneReasonCurrentTailCovered   = "current tail already satisfies limits"
	chainFreezerTailPruneReasonMissingColdCoverage  = "missing cold chain-freezer coverage"
	chainFreezerTailPruneReasonMissingChainIndex    = "missing cold chain-index coverage"
	chainFreezerTailPruneReasonMissingEventLogCold  = "missing cold indexed event-log coverage"

	chainFreezerTailCoverageChunk = 1024
)

// ChainFreezerTailPrunePlanInput describes the inclusive stage progress and
// freezer bounds needed to plan a minimal-mode ancient tail advance.
type ChainFreezerTailPrunePlanInput struct {
	// CurrentTail is the current freezer virtual tail: rows below it are
	// already hidden.
	CurrentTail uint64

	// AncientHead is the freezer append head/item count. TruncateTail must not
	// advance past this value.
	AncientHead uint64

	// HeadBlock is the canonical block number used for the recent-block
	// retention window.
	HeadBlock uint64

	// RetainBlocks is the number of recent canonical blocks that must remain
	// readable from local hot/ancient storage. Zero disables pruning.
	RetainBlocks uint64

	// ChainFreezerBlock is the highest block whose numbered chain rows are
	// known to be covered by the local freezer.
	ChainFreezerBlock    uint64
	HasChainFreezerBlock bool

	// ChainLookupPruneBlock is the highest block whose hot hash lookup rows
	// have been pruned only after verified cold chain-index coverage existed.
	ChainLookupPruneBlock    uint64
	HasChainLookupPruneBlock bool

	// EventLogBuildBlock is the highest block whose logs have been published
	// into cold event-log segments plus event-log-index sidecars. Minimal-mode
	// tail pruning must not hide local freezer rows beyond this boundary, or
	// archive log queries would be forced back to block/TransactionRet scans.
	EventLogBuildBlock    uint64
	HasEventLogBuildBlock bool
}

// ChainFreezerTailPrunePlan is a dry-run result for a possible TruncateTail call.
type ChainFreezerTailPrunePlan struct {
	CanPrune bool
	Reason   string

	CurrentTail uint64
	TargetTail  uint64
	AncientHead uint64

	// CoverageTail is the largest tail allowed by local freezer coverage,
	// verified lookup-prune progress, and indexed cold event-log coverage.
	CoverageTail uint64

	// RetentionTail is the largest tail allowed by the recent-block window.
	RetentionTail uint64

	// CoverageBlock is the inclusive block boundary behind CoverageTail.
	CoverageBlock    uint64
	HasCoverageBlock bool
}

type ChainFreezerTailPruner interface {
	rawdb.AncientReader
	Tail() (uint64, error)
	TruncateTail(items uint64) (uint64, error)
}

type ChainFreezerTailPruneApplyResult struct {
	Plan            ChainFreezerTailPrunePlan
	Applied         bool
	OldTail         uint64
	NewTail         uint64
	PrunedTailFiles uint64
}

type chainFreezerTailFilePruner interface {
	PruneTailFiles() (uint64, error)
}

type chainFreezerRangeCoverer interface {
	ChainFreezerRangeCovered(fromBlock, toBlock uint64) (bool, error)
}

type chainIndexRangeCoverer interface {
	ChainIndexRangeCovered(fromBlock, toBlock uint64) (bool, error)
}

type eventLogIndexedRangeCoverer interface {
	EventLogIndexedRangeCovered(fromBlock, toBlock uint64) (bool, error)
}

// PlanChainFreezerTailPrune computes the highest freezer virtual tail that can
// be requested without passing local ancient coverage, verified cold lookup
// coverage, the append head, or the recent-block retention window. It performs
// no writes; callers must still verify cold chain-freezer coverage before
// applying the returned TargetTail.
func PlanChainFreezerTailPrune(input ChainFreezerTailPrunePlanInput) ChainFreezerTailPrunePlan {
	plan := ChainFreezerTailPrunePlan{
		Reason:      chainFreezerTailPruneReasonReady,
		CurrentTail: input.CurrentTail,
		TargetTail:  input.CurrentTail,
		AncientHead: input.AncientHead,
	}
	if !input.HasChainFreezerBlock {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonMissingFreezerStage)
	}
	if !input.HasChainLookupPruneBlock {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonMissingLookupStage)
	}
	if !input.HasEventLogBuildBlock {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonMissingEventLogStage)
	}
	if input.RetainBlocks == 0 {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonZeroRetainBlocks)
	}
	if input.AncientHead == 0 {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonEmptyAncientStore)
	}
	if input.CurrentTail > input.AncientHead {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonTailAboveAncientHead)
	}

	coverageBlock := minUint64(minUint64(input.ChainFreezerBlock, input.ChainLookupPruneBlock), input.EventLogBuildBlock)
	coverageTail := tailAfterInclusiveBlock(coverageBlock)
	retentionTail := retainedHistoryTail(input.HeadBlock, input.RetainBlocks)
	targetTail := minUint64(coverageTail, retentionTail)
	targetTail = minUint64(targetTail, input.AncientHead)

	plan.CoverageBlock = coverageBlock
	plan.HasCoverageBlock = true
	plan.CoverageTail = coverageTail
	plan.RetentionTail = retentionTail
	plan.TargetTail = targetTail
	if targetTail <= input.CurrentTail {
		return noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonCurrentTailCovered)
	}
	plan.CanPrune = true
	return plan
}

func noChainFreezerTailPrune(plan ChainFreezerTailPrunePlan, reason string) ChainFreezerTailPrunePlan {
	plan.CanPrune = false
	plan.Reason = reason
	return plan
}

// PlanChainFreezerTailPruneFromDB reads the chain freezer, lookup-prune, and
// event-log build stages from db, then delegates to PlanChainFreezerTailPrune.
func PlanChainFreezerTailPruneFromDB(db ethdb.KeyValueReader, currentTail, ancientHead, headBlock, retainBlocks uint64) (ChainFreezerTailPrunePlan, error) {
	if db == nil {
		return ChainFreezerTailPrunePlan{}, errors.New("snapshots: nil chain freezer tail prune database")
	}
	freezerBlock, hasFreezerBlock, err := rawdb.ReadStageProgress(db, rawdb.StageChainFreezer)
	if err != nil {
		return ChainFreezerTailPrunePlan{}, err
	}
	lookupBlock, hasLookupBlock, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotChainLookupPrune)
	if err != nil {
		return ChainFreezerTailPrunePlan{}, err
	}
	eventLogBlock, hasEventLogBlock, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotEventLogBuild)
	if err != nil {
		return ChainFreezerTailPrunePlan{}, err
	}
	return PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              currentTail,
		AncientHead:              ancientHead,
		HeadBlock:                headBlock,
		RetainBlocks:             retainBlocks,
		ChainFreezerBlock:        freezerBlock,
		HasChainFreezerBlock:     hasFreezerBlock,
		ChainLookupPruneBlock:    lookupBlock,
		HasChainLookupPruneBlock: hasLookupBlock,
		EventLogBuildBlock:       eventLogBlock,
		HasEventLogBuildBlock:    hasEventLogBlock,
	}), nil
}

func ApplyChainFreezerTailPruneFromDB(db ethdb.KeyValueReader, freezer ChainFreezerTailPruner, cold rawdb.AncientReader, headBlock, retainBlocks uint64) (*ChainFreezerTailPruneApplyResult, error) {
	if freezer == nil {
		return nil, errors.New("snapshots: nil chain freezer tail pruner")
	}
	currentTail, err := freezer.Tail()
	if err != nil {
		return nil, err
	}
	ancientHead, err := freezer.AncientCount(rawdb.AncientBlocksTable)
	if err != nil {
		return nil, err
	}
	plan, err := PlanChainFreezerTailPruneFromDB(db, currentTail, ancientHead, headBlock, retainBlocks)
	if err != nil {
		return nil, err
	}
	result := &ChainFreezerTailPruneApplyResult{
		Plan:    plan,
		OldTail: currentTail,
		NewTail: currentTail,
	}
	if !plan.CanPrune {
		return result, nil
	}
	if err := verifyColdChainFreezerTailCoverage(cold, currentTail, plan.TargetTail); err != nil {
		result.Plan = noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonMissingColdCoverage)
		return result, nil
	}
	if err := verifyColdChainIndexTailCoverage(cold, currentTail, plan.TargetTail); err != nil {
		result.Plan = noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonMissingChainIndex)
		return result, nil
	}
	if err := verifyColdEventLogTailCoverage(cold, currentTail, plan.TargetTail); err != nil {
		result.Plan = noChainFreezerTailPrune(plan, chainFreezerTailPruneReasonMissingEventLogCold)
		return result, nil
	}
	oldTail, err := freezer.TruncateTail(plan.TargetTail)
	if err != nil {
		return nil, err
	}
	result.Applied = true
	result.OldTail = oldTail
	result.NewTail = plan.TargetTail
	if physical, ok := freezer.(chainFreezerTailFilePruner); ok {
		pruned, err := physical.PruneTailFiles()
		if err != nil {
			return nil, err
		}
		result.PrunedTailFiles = pruned
	}
	if plan.TargetTail > 0 {
		if writer, ok := db.(ethdb.KeyValueWriter); ok {
			if err := rawdb.WriteStageProgress(writer, rawdb.StageSnapshotChainFreezerTailPrune, plan.TargetTail-1); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func verifyColdEventLogTailCoverage(cold rawdb.AncientReader, fromTail, toTail uint64) error {
	if toTail <= fromTail {
		return nil
	}
	fromBlock := fromTail
	if fromBlock == 0 {
		fromBlock = 1
	}
	if toTail <= fromBlock {
		return nil
	}
	coverer, ok := cold.(eventLogIndexedRangeCoverer)
	if !ok {
		return rawdb.ErrNotInAncient
	}
	covered, err := coverer.EventLogIndexedRangeCovered(fromBlock, toTail-1)
	if err != nil {
		return err
	}
	if !covered {
		return rawdb.ErrNotInAncient
	}
	return nil
}

func verifyColdChainIndexTailCoverage(cold rawdb.AncientReader, fromTail, toTail uint64) error {
	if toTail <= fromTail {
		return nil
	}
	coverer, ok := cold.(chainIndexRangeCoverer)
	if !ok {
		return rawdb.ErrNotInAncient
	}
	covered, err := coverer.ChainIndexRangeCovered(fromTail, toTail-1)
	if err != nil {
		return err
	}
	if !covered {
		return rawdb.ErrNotInAncient
	}
	return nil
}

func verifyColdChainFreezerTailCoverage(cold rawdb.AncientReader, fromTail, toTail uint64) error {
	if cold == nil {
		return rawdb.ErrNotInAncient
	}
	if toTail <= fromTail {
		return nil
	}
	last := toTail - 1
	if coverer, ok := cold.(chainFreezerRangeCoverer); ok {
		covered, err := coverer.ChainFreezerRangeCovered(fromTail, last)
		if err != nil {
			return err
		}
		if !covered {
			return rawdb.ErrNotInAncient
		}
		return nil
	}
	for start := fromTail; start <= last; {
		remaining := last - start + 1
		chunk := minUint64(remaining, chainFreezerTailCoverageChunk)
		for _, table := range []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable} {
			rows, err := cold.AncientRange(table, start, chunk, 0)
			if err != nil {
				return err
			}
			if uint64(len(rows)) != chunk {
				return rawdb.ErrNotInAncient
			}
		}
		prev := start
		start += chunk
		if start <= prev {
			break
		}
	}
	return nil
}

func retainedHistoryTail(headBlock, retainBlocks uint64) uint64 {
	if retainBlocks == 0 || headBlock < retainBlocks {
		return 0
	}
	return headBlock - retainBlocks + 1
}

func tailAfterInclusiveBlock(blockNum uint64) uint64 {
	if blockNum == ^uint64(0) {
		return blockNum
	}
	return blockNum + 1
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
