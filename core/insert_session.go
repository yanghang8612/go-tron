package core

import (
	"fmt"

	"github.com/tronprotocol/go-tron/core/types"
)

// InsertSession applies a sequence of block batches that share ONE canonical
// range executor. It is the cross-chunk range vehicle for the sync drain loop,
// which feeds <=100-block batches: holding one session across them reuses the
// StateDB and CommitScope instead of reopening both at every local chunk
// boundary. Under deep pipelining (commit depth > 2), it additionally defers the
// commit-worker drain until Finish so the pipeline is not forced empty between
// batches.
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
type InsertSession struct {
	bc                *BlockChain
	executor          *canonicalRangeExecutor
	applied           bool
	historyIndexReady bool
	// yieldChainLock lets bulk sync hand archive RPCs the canonical lock after
	// every committed block without giving up the reusable range executor.
	// Ordinary sessions keep their existing whole-batch atomicity.
	yieldChainLock bool
	// chainLockYieldHook is test-only observability for the unlocked handoff.
	chainLockYieldHook func()
}

// BeginInsertSession starts a cross-batch insert session sharing one canonical
// range executor. Finish must be called once the batch sequence is done (drains
// the commit worker and converges the on-disk image).
func (bc *BlockChain) BeginInsertSession() *InsertSession {
	return &InsertSession{bc: bc, executor: newCanonicalRangeExecutor(bc, true)}
}

// BeginSyncInsertSession starts a bulk-sync session whose tx-hash lookup and
// state-history posting rows are emitted by recoverable sorted stages after
// canonical execution has finished. Per-block TransactionRet and authoritative
// changeset rows remain on the normal canonical path.
func (bc *BlockChain) BeginSyncInsertSession() *InsertSession {
	executor := newCanonicalRangeExecutorWithOptions(bc, true, nil, true)
	executor.enableStateReadAhead()
	return &InsertSession{bc: bc, executor: executor, yieldChainLock: true}
}

// PrepareDecodedBlocks starts deterministic state read ahead as soon as a
// staged body batch has been decoded. It does not validate or mutate state;
// canonical insertion remains the sole ordered accept/reject path.
func (s *InsertSession) PrepareDecodedBlocks(blocks []*types.Block) int {
	if s == nil || s.executor == nil {
		return 0
	}
	return s.executor.PrepareReadAhead(blocks)
}

// PrepareDecodedBlock is the downloader fast path. It uses the retained wire
// size for queue accounting instead of traversing the decoded protobuf again.
func (s *InsertSession) PrepareDecodedBlock(block *types.Block, encodedBytes int) bool {
	return s != nil && s.executor != nil && s.executor.PrepareReadAheadBlock(block, encodedBytes)
}

// Insert applies one batch within the session WITHOUT settling (no scope close
// and, when async commit is active, no commit-worker drain) — that is deferred
// to Finish so the range state survives local chunk boundaries. Acquires chainmu
// for this batch only. On the first failing block it returns an
// *InsertBlocksError (Index within THIS batch); the caller breaks the batch loop
// and calls Finish, which closes the scope and surfaces any worker-side commit
// error. Ordinary sessions hold chainmu for the batch; bulk-sync sessions
// release it after each fully published block so archive readers do not queue
// behind the downloader's whole local batch.
func (s *InsertSession) Insert(blocks []*types.Block) error {
	return s.InsertBlocksWithStageHook(blocks, nil)
}

// InsertBlocksWithStageHook applies one batch within the session and forwards
// canonical stage progress to hook, matching BlockChain.InsertBlocksWithStageHook
// while keeping the cross-batch executor alive until Finish.
func (s *InsertSession) InsertBlocksWithStageHook(blocks []*types.Block, hook StageProgressHook) error {
	return s.InsertBlocksWithStageHookPrewarmed(blocks, hook, nil)
}

// InsertBlocksWithStageHookPrewarmed reuses immutable preprocessing started by
// the downloader for this batch. It is otherwise identical to
// InsertBlocksWithStageHook and retains the same ordered validation boundary.
func (s *InsertSession) InsertBlocksWithStageHookPrewarmed(blocks []*types.Block, hook StageProgressHook, prewarm *SignaturePrewarmRun) error {
	if len(blocks) == 0 {
		prewarm.Wait()
		return nil
	}
	// Start signature recovery for the whole batch, then immediately execute the
	// first block. Workers stay ahead on later blocks while state execution runs;
	// sync.Once makes an early serial read wait for only its own recovery. Join
	// before returning so no worker retains the caller's batch.
	sigPrewarm := s.bc.signaturePrewarmForBlocks(blocks, prewarm)
	defer sigPrewarm.Wait()
	prevHook := s.executor.stageHook
	s.executor.stageHook = hook
	defer func() { s.executor.stageHook = prevHook }()
	if !s.yieldChainLock {
		s.bc.chainmu.Lock()
		defer s.bc.chainmu.Unlock()
		if err := s.prepareInsertLocked(); err != nil {
			return err
		}
		return s.insertBlocksLocked(blocks)
	}

	// Sync is the sole canonical writer, so the executor/state/scope can remain
	// live while chainmu is handed to read-only archive requests between blocks.
	// Each block is still fully atomic: validation, execution, metadata, head,
	// and the readable archive boundary are all published before the unlock.
	for i, block := range blocks {
		s.bc.chainmu.Lock()
		err := s.prepareInsertLocked()
		if err == nil {
			err = s.bc.insertBlockLockedWithExecutor(block, s.executor)
		}
		s.bc.chainmu.Unlock()
		if err != nil {
			var blockNum uint64
			if block != nil {
				blockNum = block.Number()
			}
			return &InsertBlocksError{Index: i, BlockNumber: blockNum, Err: err}
		}
		s.applied = true
		if i+1 < len(blocks) && s.chainLockYieldHook != nil {
			s.chainLockYieldHook()
		}
	}
	return nil
}

func (s *InsertSession) prepareInsertLocked() error {
	if s.bc.closed.Load() {
		return ErrBlockChainClosed
	}
	if s.executor.deferStateHistoryIndex && !s.historyIndexReady {
		if err := s.bc.ensureStateHistoryIndexStageLocked(); err != nil {
			return err
		}
		s.historyIndexReady = true
	}
	return nil
}

func (s *InsertSession) insertBlocksLocked(blocks []*types.Block) error {
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

// FlushLatest preserves the ordinary synchronous InsertBlocks chunk boundary:
// it makes latest-domain writes visible through the block buffer while retaining
// the StateDB, CommitScope, and shared domain transaction for the next Insert.
// Deep async sessions deliberately defer this to their existing worker/barrier
// flow and Finish.
func (s *InsertSession) FlushLatest() error {
	if s == nil || s.bc == nil || s.executor == nil {
		return nil
	}
	s.bc.chainmu.Lock()
	defer s.bc.chainmu.Unlock()
	if s.bc.closed.Load() {
		return ErrBlockChainClosed
	}
	return s.executor.FlushLatest()
}

// Finish drains the commit worker when active, flushes the executor's scope into
// the committed buffer layers, and flushes buffer layers up to the solidified
// height. This converges to the ordinary per-range on-disk image, so a later
// reorg discards/rewinds identical state. It runs once at the end of a session
// rather than once per local chunk. Acquires chainmu; safe to call after an
// Insert error (it still drains + surfaces any worker error). Idempotent enough
// to call once on every drain-loop exit path.
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
