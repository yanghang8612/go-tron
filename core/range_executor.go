package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
)

type canonicalBlockExecution struct {
	state    *state.StateDB
	commit   *state.CommitScope
	txRange  *rawdb.StateTxRange
	pipeline *canonicalStagePipeline
	// parent is the range-local tip this block builds on — the block the
	// executor last applied, NOT necessarily bc.CurrentBlock(). With async
	// commit off they are identical (the foreground advances currentBlock
	// synchronously); with async commit on the published currentBlock lags the
	// range tip, so applyBlockWithPlan must use this captured parent for
	// parent-linkage and parent-state-root reads. Nil falls back to
	// bc.CurrentBlock() for callers that do not plan a range.
	parent *types.Block
	// parentDynProps threads the previous block's finalized dynamic properties
	// into this block under async commit (decision-b), so execution never reads
	// the lazily-published dynPropsCache. nil ⇒ read the head cache as usual
	// (the synchronous path and the first block of a range). finalDynProps is
	// the output: this block's finalized DP, which the executor carries forward
	// as the next block's parentDynProps.
	parentDynProps *state.DynamicProperties
	finalDynProps  *state.DynamicProperties
	// txInfoBatch is range-owned receipt storage. The synchronous path returns
	// it after applyBlockWithPlan; async commit marks handedOff and returns it
	// from the worker only after metadata serialization.
	txInfoBatch          *transactionInfoBatch
	txInfoBatchPool      *transactionInfoBatchPool
	txInfoBatchHandedOff bool
}

type canonicalCommitResult struct {
	Root  tcommon.Hash
	Stats state.CommitStats
}

func (p *canonicalBlockExecution) Validate(block *types.Block, historyEnabled bool) error {
	if p == nil {
		return fmt.Errorf("canonical block execution: nil plan")
	}
	if block == nil {
		return fmt.Errorf("canonical block execution: nil block")
	}
	if p.state == nil {
		return fmt.Errorf("canonical block execution: nil state")
	}
	if p.pipeline == nil {
		return fmt.Errorf("canonical block execution: nil stage pipeline")
	}
	if historyEnabled {
		if p.txRange == nil {
			return fmt.Errorf("canonical block execution: missing planned state tx range for history-enabled block %d", block.Number())
		}
		if p.txRange.BlockNum != block.Number() || p.txRange.BlockHash != block.Hash() {
			return fmt.Errorf("canonical block execution: planned state tx range mismatch: got block %d %x want %d %x", p.txRange.BlockNum, p.txRange.BlockHash, block.Number(), block.Hash())
		}
	}
	return nil
}

func (p *canonicalBlockExecution) BeginDomainChangeStage(writer ethdb.KeyValueWriter) (*state.DomainChangeStage, error) {
	if p == nil || p.txRange == nil {
		return nil, nil
	}
	return p.state.BeginDomainChangeStage(writer, p.txRange)
}

func (p *canonicalBlockExecution) Commit(opts state.CommitOptions) (tcommon.Hash, state.CommitStats, error) {
	if p == nil || p.state == nil {
		return tcommon.Hash{}, state.CommitStats{}, fmt.Errorf("canonical block execution: nil state")
	}
	if p.commit != nil {
		return p.state.CommitWithStatsOptionsInScope(p.commit, opts)
	}
	return p.state.CommitWithStatsOptions(opts)
}

func (p *canonicalBlockExecution) CommitState(writer ethdb.KeyValueWriter, block *types.Block, opts state.CommitOptions, checkpoint bool) (canonicalCommitResult, error) {
	if p == nil {
		return canonicalCommitResult{}, fmt.Errorf("canonical block execution: nil plan")
	}
	if block == nil {
		return canonicalCommitResult{}, fmt.Errorf("canonical block execution: nil block")
	}
	if p.pipeline == nil {
		return canonicalCommitResult{}, fmt.Errorf("canonical block execution: nil stage pipeline")
	}
	if checkpoint && writer == nil {
		return canonicalCommitResult{}, fmt.Errorf("canonical block execution: nil checkpoint writer")
	}
	opts.BlockNumber = block.Number()
	if p.commit != nil {
		opts.FlushLatestDomain = func() error { return nil }
	}
	root, stats, err := p.Commit(opts)
	if err != nil {
		return canonicalCommitResult{}, fmt.Errorf("commit state: %w", err)
	}
	if err := p.finishCommitState(writer, block, root, checkpoint); err != nil {
		return canonicalCommitResult{}, err
	}
	return canonicalCommitResult{Root: root, Stats: stats}, nil
}

// finishCommitState writes the commitment checkpoint (when enabled) and advances
// the StageCommitment progress row. Shared by the synchronous CommitState and
// the async commit worker (which folds first, then calls this with the computed
// root and a writer bound to the committing block's buffer layer). Pulling it
// out keeps the two paths from drifting.
func (p *canonicalBlockExecution) finishCommitState(writer ethdb.KeyValueWriter, block *types.Block, root tcommon.Hash, checkpoint bool) error {
	if checkpoint {
		cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetCommitmentCheckpoint)
		if !ok || cfg.WriteHotCommitmentCheckpoint == nil {
			return fmt.Errorf("commitment checkpoint domain writer unavailable")
		}
		if err := cfg.WriteHotCommitmentCheckpoint(writer, &rawdb.StateCommitmentCheckpoint{
			BlockNum:  block.Number(),
			BlockHash: block.Hash(),
			Root:      root,
			Scheme:    rawdb.LatestDomainCommitmentScheme,
		}); err != nil {
			return fmt.Errorf("write domain commitment checkpoint: %w", err)
		}
	}
	if err := p.pipeline.Advance(rawdb.StageCommitment); err != nil {
		return err
	}
	return nil
}

// CommitStateCapture is the async-commit foreground half of CommitState: it
// writes the latest-domain rows to the in-memory scope and captures the
// commitment-fold inputs WITHOUT folding (the plan's StateDB must be in
// deferFold mode). It does NOT write the checkpoint or advance StageCommitment —
// those need the fold root and run on the commit worker via finishCommitState.
// The caller consumes the captured fold via StateDB.TakeCapturedFold.
func (p *canonicalBlockExecution) CommitStateCapture(block *types.Block, opts state.CommitOptions) (state.CommitStats, error) {
	if p == nil || p.state == nil {
		return state.CommitStats{}, fmt.Errorf("canonical block execution: nil plan")
	}
	if block == nil {
		return state.CommitStats{}, fmt.Errorf("canonical block execution: nil block")
	}
	if p.pipeline == nil {
		return state.CommitStats{}, fmt.Errorf("canonical block execution: nil stage pipeline")
	}
	p.state.SetDeferFold(true)
	opts.BlockNumber = block.Number()
	if p.commit != nil {
		opts.FlushLatestDomain = func() error { return nil }
	}
	_, stats, err := p.Commit(opts)
	if err != nil {
		return stats, fmt.Errorf("commit state capture: %w", err)
	}
	return stats, nil
}

func (p *canonicalBlockExecution) FlushLatestUpTo(cutoff int64) error {
	if p == nil || p.commit == nil || cutoff <= 0 {
		return nil
	}
	return p.commit.FlushLatestUpTo(uint64(cutoff))
}

// canonicalRangeExecutor owns the reusable state surfaces for one canonical
// range application. InsertBlocks, fork replay, and restart replay should all
// enter block execution through this object so state opening, txNum planning,
// commit-scope reuse, and per-block stage progress stay on one staged path.
type canonicalRangeExecutor struct {
	bc                *BlockChain
	allowSharedCommit bool
	state             *state.StateDB
	commit            *state.CommitScope
	txRanges          *stateTxRangeAllocator
	// tipBlock is the range-local tip: the block this executor last applied
	// successfully. nil means "not yet advanced in this range" → tip() falls
	// back to bc.CurrentBlock(). Reset/Abort clear it. With async commit off,
	// tip() always equals bc.CurrentBlock() because the foreground advances
	// currentBlock synchronously inside applyBlockWithPlan.
	tipBlock *types.Block
	// lastDynProps carries the previous block's finalized dynamic properties so
	// the next block threads them directly under async commit (decision-b),
	// rather than reading the lazily-published dynPropsCache. Only populated when
	// bc.asyncCommit; nil for the synchronous path (which reads the head cache).
	lastDynProps  *state.DynamicProperties
	txInfoBatches *transactionInfoBatchPool
}

func newCanonicalRangeExecutor(bc *BlockChain, allowSharedCommit bool) *canonicalRangeExecutor {
	depth := 1
	if bc != nil && bc.asyncCommit {
		depth = bc.commitDepth
	}
	return &canonicalRangeExecutor{
		bc:                bc,
		allowSharedCommit: allowSharedCommit,
		txInfoBatches:     newTransactionInfoBatchPool(depth),
	}
}

// tip returns the range-local tip — the block subsequent applies build on.
// Equals bc.CurrentBlock() until the executor has applied at least one block in
// this range (and, with async commit off, forever after, since currentBlock is
// advanced synchronously).
func (e *canonicalRangeExecutor) tip() *types.Block {
	if e != nil && e.tipBlock != nil {
		return e.tipBlock
	}
	return e.bc.CurrentBlock()
}

func (e *canonicalRangeExecutor) Apply(block *types.Block) error {
	if e == nil || e.bc == nil {
		return fmt.Errorf("canonical range executor: nil executor")
	}
	if block == nil {
		return fmt.Errorf("canonical range executor: nil block")
	}
	bc := e.bc
	current := e.tip()
	if e.state == nil {
		statedb, err := bc.openCurrentState()
		if err != nil {
			return fmt.Errorf("open state: %w", err)
		}
		e.state = statedb
	}
	if e.txRanges == nil {
		txRanges, err := bc.newStateTxRangeAllocator(current.Number())
		if err != nil {
			return err
		}
		e.txRanges = txRanges
	}
	plannedTxRange, err := e.txRanges.next(block)
	if err != nil {
		return err
	}
	if e.allowSharedCommit && e.commit == nil {
		if bc.stateCommitScopeHook != nil {
			bc.stateCommitScopeHook()
		}
		e.commit = e.state.NewCommitScope()
	}
	plan := &canonicalBlockExecution{
		state:           e.state,
		commit:          e.commit,
		txRange:         plannedTxRange,
		pipeline:        newCanonicalStagePipeline(bc.buffer, block.Number(), block.Hash()),
		parent:          current,
		txInfoBatch:     e.txInfoBatches.acquire(),
		txInfoBatchPool: e.txInfoBatches,
	}
	defer func() {
		if !plan.txInfoBatchHandedOff {
			plan.txInfoBatchPool.release(plan.txInfoBatch)
		}
	}()
	// Under async commit, thread the previous block's finalized dynamic
	// properties into this block (decision-b). Left nil for the synchronous
	// path so applyBlockWithPlan reads the head cache exactly as before.
	if bc.asyncCommit {
		plan.parentDynProps = e.lastDynProps
	}
	if err := bc.applyBlockWithPlan(block, plan); err != nil {
		return err
	}
	// Advance the range-local tip. With async commit off this matches
	// bc.CurrentBlock() (already stored synchronously inside applyBlockWithPlan);
	// it is the load-bearing tip only once async commit lets currentBlock lag.
	e.tipBlock = block
	if bc.asyncCommit {
		e.lastDynProps = plan.finalDynProps
	}
	return nil
}

func (e *canonicalRangeExecutor) Reset() {
	if e == nil {
		return
	}
	if e.commit != nil {
		e.commit.Discard()
	}
	e.state = nil
	e.commit = nil
	e.txRanges = nil
	e.tipBlock = nil
	e.lastDynProps = nil
}

func (e *canonicalRangeExecutor) Abort() error {
	if e == nil {
		return nil
	}
	var err error
	if e.commit != nil {
		err = e.commit.Abort()
	}
	e.state = nil
	e.commit = nil
	e.txRanges = nil
	e.tipBlock = nil
	e.lastDynProps = nil
	return err
}

func (e *canonicalRangeExecutor) Close() error {
	if e == nil || e.commit == nil {
		return nil
	}
	err := e.commit.Close()
	e.commit = nil
	return err
}
