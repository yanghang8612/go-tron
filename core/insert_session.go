package core

import (
	"fmt"

	"github.com/tronprotocol/go-tron/core/types"
)

// InsertSession applies a sequence of block batches that share ONE canonical
// range executor and, under deep pipelining (commit depth > 2), defer the
// commit-worker drain until Finish — so the pipeline is not forced empty at every
// batch boundary. It is the cross-batch barrier-amortization vehicle for the sync
// drain loop, which feeds ≤100-block batches: holding one session across them lets
// the commit worker lag past a batch boundary instead of being drained at each.
//
// Reusing the executor across Insert calls carries its tip (parent linkage, since
// the published CurrentBlock lags under async commit), its lastDynProps
// (decision-(b): threads the previous block's finalized dynamic properties forward
// so a batch's FIRST block never falls back to the lazily-published, possibly-stale
// dynPropsCache — this is exactly the freshness the old per-range
// WaitForCommitSettled barrier provided by draining), and its in-memory commit
// scope (bounded by the per-block FlushLatestUpTo of the committed+solidified
// prefix).
//
// Concurrency: each Insert/Finish acquires chainmu only for its own call; between
// calls chainmu is free and the commit worker keeps committing. During a sync
// drain the SyncService is the sole canonical writer (the producer is witness-mode
// only and inactive during sync), and reorgs happen INSIDE insertBlockLockedWithExecutor
// (switchFork, which Reset()s this shared executor) — never between Insert calls —
// so the carried executor state is only mutated under chainmu inside Insert.
//
// Created via BeginInsertSession only on the deep path (PipelinedCommitDepth() > 2).
// The synchronous and depth-2 paths keep using InsertBlocks unchanged.
type InsertSession struct {
	bc       *BlockChain
	executor *canonicalRangeExecutor
	applied  bool
}

// BeginInsertSession starts a cross-batch insert session sharing one canonical
// range executor. Finish must be called once the batch sequence is done (drains
// the commit worker and converges the on-disk image).
func (bc *BlockChain) BeginInsertSession() *InsertSession {
	return &InsertSession{bc: bc, executor: newCanonicalRangeExecutor(bc, true)}
}

// Insert applies one batch within the session WITHOUT settling (no commit-worker
// drain, no scope close, no postFlush) — that is deferred to Finish so the
// pipeline stays full across batch boundaries. Acquires chainmu for this batch
// only. On the first failing block it returns an *InsertBlocksError (Index within
// THIS batch); the caller breaks the batch loop and calls Finish, which drains
// and surfaces any worker-side commit error.
func (s *InsertSession) Insert(blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	s.bc.chainmu.Lock()
	defer s.bc.chainmu.Unlock()
	if s.bc.closed.Load() {
		return ErrBlockChainClosed
	}
	// Warm signature recovery for this batch (off the critical path), matching
	// insertBlocksLocked. Pure cache-warming.
	prewarmBlockSignatures(blocks, s.bc.headerSigPrewarmer())
	for i, block := range blocks {
		if err := s.bc.insertBlockLockedWithExecutor(block, s.executor); err != nil {
			var blockNum uint64
			if block != nil {
				blockNum = block.Number()
			}
			return &InsertBlocksError{Index: i, BlockNumber: blockNum, Err: err}
		}
		s.applied = true
	}
	return nil
}

// Finish drains the commit worker, flushes the executor's scope into the committed
// buffer layers, and flushes buffer layers up to the solidified height — converging
// to the synchronous on-disk image so a subsequent reorg discards/rewinds identical
// state. Mirrors insertBlocksLocked's async defer, but runs once at the end of the
// session rather than once per batch. Acquires chainmu; safe to call after an Insert
// error (it still drains + surfaces the worker error). Idempotent enough to call once
// on every drain-loop exit path.
func (s *InsertSession) Finish() (err error) {
	s.bc.chainmu.Lock()
	defer s.bc.chainmu.Unlock()
	s.bc.WaitForCommitSettled()
	if errPtr := s.bc.commitErr.Load(); errPtr != nil && err == nil {
		err = fmt.Errorf("async commit failed: %w", *errPtr)
	}
	if closeErr := s.executor.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err == nil && s.applied {
		if solidified := s.bc.cachedDynProps().LatestSolidifiedBlockNum(); solidified > 0 {
			if flushErr := s.bc.postFlush(solidified); flushErr != nil {
				err = flushErr
			}
		}
	}
	return err
}
