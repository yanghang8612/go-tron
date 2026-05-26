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
	if p.commit != nil {
		opts.FlushLatestDomain = func() error { return nil }
	}
	root, stats, err := p.Commit(opts)
	if err != nil {
		return canonicalCommitResult{}, fmt.Errorf("commit state: %w", err)
	}
	if checkpoint {
		cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetCommitmentCheckpoint)
		if !ok || cfg.WriteHotCommitmentCheckpoint == nil {
			return canonicalCommitResult{}, fmt.Errorf("commitment checkpoint domain writer unavailable")
		}
		if err := cfg.WriteHotCommitmentCheckpoint(writer, &rawdb.StateCommitmentCheckpoint{
			BlockNum:  block.Number(),
			BlockHash: block.Hash(),
			Root:      root,
			Scheme:    rawdb.LatestDomainCommitmentScheme,
		}); err != nil {
			return canonicalCommitResult{}, fmt.Errorf("write domain commitment checkpoint: %w", err)
		}
	}
	if err := p.pipeline.Advance(rawdb.StageCommitment); err != nil {
		return canonicalCommitResult{}, err
	}
	return canonicalCommitResult{Root: root, Stats: stats}, nil
}

func (p *canonicalBlockExecution) FlushLatestUpTo(cutoff int64, numberOf func(tcommon.Hash) (uint64, bool)) error {
	if p == nil || p.commit == nil || cutoff <= 0 {
		return nil
	}
	return p.commit.FlushLatestUpTo(uint64(cutoff), numberOf)
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
}

func newCanonicalRangeExecutor(bc *BlockChain, allowSharedCommit bool) *canonicalRangeExecutor {
	return &canonicalRangeExecutor{bc: bc, allowSharedCommit: allowSharedCommit}
}

func (e *canonicalRangeExecutor) Apply(block *types.Block) error {
	if e == nil || e.bc == nil {
		return fmt.Errorf("canonical range executor: nil executor")
	}
	if block == nil {
		return fmt.Errorf("canonical range executor: nil block")
	}
	bc := e.bc
	current := bc.CurrentBlock()
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
		state:    e.state,
		commit:   e.commit,
		txRange:  plannedTxRange,
		pipeline: newCanonicalStagePipeline(bc.buffer, block.Number(), block.Hash()),
	}
	return bc.applyBlockWithPlan(block, plan)
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
