package core

import (
	"fmt"
	"os"
	"strconv"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Async-commit pipeline depth D bounds how many blocks the exec foreground may
// run ahead of the serial commit worker. The buffered commit queue holds D-2
// jobs (worker processing 1 + queue D-2 + foreground's just-begun layer 1 = D
// in-flight buffer layers, matching SetMaxInflight(D)); the send blocking on a
// full queue is the backpressure that keeps BeginBlock from exceeding maxInflight.
//
//   - D == 2  → cap 0 (unbuffered rendezvous), maxInflight 2 — EXACTLY today.
//   - D  > 2  → buffered + the cross-batch barrier-amortization path (see
//     pipelinedCommit / InsertSession). Enabled ops-only, never wire-observable.
//
// The depth is resolved ONCE at NewBlockChain (so the commit worker, started in
// the constructor, ranges a correctly-sized channel and is never orphaned by a
// later re-make). SetAsyncCommit only toggles the buffer's in-flight cap.
const (
	defaultCommitPipelineDepth = 2
	maxCommitPipelineDepth     = 16
)

// resolveCommitPipelineDepth reads the ops-only GTRON_ASYNC_COMMIT_DEPTH override,
// clamped to [defaultCommitPipelineDepth, maxCommitPipelineDepth]. Unset, invalid,
// or below the floor → the default (2 = today's behavior). It is never gated on
// chain config / proposals — it changes only the internal commit schedule.
func resolveCommitPipelineDepth() int {
	v := os.Getenv("GTRON_ASYNC_COMMIT_DEPTH")
	if v == "" {
		return defaultCommitPipelineDepth
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < defaultCommitPipelineDepth {
		return defaultCommitPipelineDepth
	}
	if n > maxCommitPipelineDepth {
		return maxCommitPipelineDepth
	}
	return n
}

// commitJob is one block's deferred commit: the captured commitment fold plus
// the inputs the publish tail consumes. Everything here is either immutable
// (block, txInfos, root-to-be) or a deep snapshot taken at handoff (dynProps,
// cycleRewards) so the worker never reads foreground-mutable state of a LATER
// block — see commitAsync.
type commitJob struct {
	plan              *canonicalBlockExecution
	block             *types.Block
	captured          *state.CapturedCommit
	layer             blockbuffer.InflightHandle
	dynProps          *state.DynamicProperties
	cycleRewards      cycleRewardAccumulatorSnapshot
	txInfos           []*corepb.TransactionInfo
	wasMaintenance    bool
	maintNewWitnesses []tcommon.Address
	checkpoint        bool
}

// SetAsyncCommit enables (or disables) the async/pipelined commit path. It must
// be called before the chain starts inserting blocks (e.g. immediately after
// NewBlockChain), never concurrently with insertion. Default OFF — the
// synchronous, byte-identical commit path runs unless this is set true.
//
// Enabling raises the buffer to bc.commitDepth in-flight layers so the worker can
// write committing blocks' layers while the foreground writes later ones (depth 2
// = today: one committing, one executing). The depth itself was resolved at
// NewBlockChain and the commit queue sized to depth-2 there, so this only flips
// the buffer's in-flight cap. It is deliberately NOT wired to any chain-config /
// proposal value: async commit only changes the internal commit *schedule*, never
// any wire-observable byte, so it must never be visible on the network.
func (bc *BlockChain) SetAsyncCommit(enabled bool) {
	bc.asyncCommit = enabled
	if enabled {
		bc.buffer.SetMaxInflight(bc.commitDepth)
	} else {
		bc.buffer.SetMaxInflight(1)
	}
}

// PipelinedCommitDepth returns the configured async-commit pipeline depth (≥2)
// when async commit is enabled, else 0. Used by the sync drain loop to decide
// whether to span one InsertSession across batches (depth > 2) for cross-batch
// barrier amortization.
func (bc *BlockChain) PipelinedCommitDepth() int {
	if !bc.asyncCommit {
		return 0
	}
	return bc.commitDepth
}

// pipelinedCommit reports whether the deep/amortized path is active (async commit
// on AND depth > 2). At depth 2 the legacy per-range path runs unchanged.
func (bc *BlockChain) pipelinedCommit() bool {
	return bc.asyncCommit && bc.commitDepth > 2
}

// commitFoldHook, when non-nil, is invoked by the commit worker for each block
// just before the fold; a non-nil error it returns is treated as a worker fold
// failure (fail-fast + DiscardInflight). TEST-ONLY (set via
// SetCommitFoldHookForTest); nil in production.
var commitFoldHook func(blockNum uint64) error

// SetCommitFoldHookForTest installs the commit-worker fold hook. Pass nil to
// clear. TEST-ONLY — production never calls this.
func SetCommitFoldHookForTest(fn func(blockNum uint64) error) { commitFoldHook = fn }

// startCommitWorker spawns the serial commit goroutine. Like the flush worker
// it is started once construction can no longer fail. It is idle (blocked on
// the channel) until async commit is enabled and a job is enqueued, so it costs
// nothing when async commit is off.
func (bc *BlockChain) startCommitWorker() {
	bc.commitWorkerWg.Add(1)
	go func() {
		defer bc.commitWorkerWg.Done()
		for job := range bc.commitQueue {
			bc.runCommitJob(job)
		}
	}()
}

// commitAsync is the async-commit foreground half, invoked from
// applyBlockWithPlan once every shared step (exec, maintenance, rooted-DP flush,
// TAPOS/tx-count) has run. It writes the latest-domain rows into the in-memory
// scope and captures the commitment-fold inputs WITHOUT folding, snapshots the
// foreground-mutable state the publish tail will consume, hands the job to the
// serial commit worker, and runs the (scope-owned, solidified-lagging) latest +
// buffer-layer flushes itself. Returns once the job is enqueued; the worker
// produces the root, advances the head, fires hooks, and commits the layer.
//
// Callers hold chainmu.
func (bc *BlockChain) commitAsync(
	block *types.Block,
	plan *canonicalBlockExecution,
	statedb *state.StateDB,
	dynProps *state.DynamicProperties,
	stats *applyStats,
	commitOpts state.CommitOptions,
	wasMaintenanceBlock bool,
	maintNewWitnesses []tcommon.Address,
	rewardAcctAddrs []tcommon.Address,
	txInfos []*corepb.TransactionInfo,
) error {
	// 1. Write latest-domain rows to the scope + capture the fold inputs.
	commitStats, err := plan.CommitStateCapture(block, commitOpts)
	if err != nil {
		return err
	}
	captured := statedb.TakeCapturedFold()
	if captured == nil {
		return fmt.Errorf("async commit: deferred commit produced no captured fold")
	}
	stats.StateCommitDetail = commitStats
	stats.mark(&stats.StateCommit)

	// 2. Refresh the system/reward account caches FOREGROUND, reading the
	//    StateDB as of THIS block before the next block mutates the reused
	//    StateDB. (The synchronous path does this after the fold; the fold does
	//    not change account values, so reading here is value-identical.)
	bc.updateSystemAccountCache(statedb)
	bc.updateRewardAccountCache(statedb, rewardAcctAddrs)

	// 3. Capture the in-flight layer this block owns and re-point the stage
	//    pipeline at it, so the worker's post-execution stage advances
	//    (StageCommitment, StageFinish) land in THIS block's layer rather than
	//    whatever layer is newest when the worker runs.
	hN, ok := bc.buffer.NewestInflight()
	if !ok {
		return fmt.Errorf("async commit: no in-flight buffer layer to commit")
	}
	plan.pipeline.SetWriter(bc.buffer.LayerWriter(hN))

	// 4. Snapshot the foreground-mutable in-memory state the publish tail reads,
	//    so the worker never observes a LATER block's value:
	//      - dynProps: a Copy (decision-(b)); the worker also publishes it to
	//        dynPropsCache, so ProcessOnBlock(N) reads block N's DP.
	//      - cycleRewards: a deep snapshot of the pending accumulator.
	//    block / root / txInfos are immutable.
	job := &commitJob{
		plan:              plan,
		block:             block,
		captured:          captured,
		layer:             hN,
		dynProps:          dynProps.Copy(),
		cycleRewards:      bc.cycleRewards.Snapshot(),
		txInfos:           txInfos,
		wasMaintenance:    wasMaintenanceBlock,
		maintNewWitnesses: maintNewWitnesses,
		checkpoint:        bc.config.StateCommitmentCheckpoints,
	}

	// 5. Hand the fold + publish tail to the serial commit worker (rendezvous;
	//    bounds the pipeline to depth 2). After this returns the foreground may
	//    begin the next block's layer.
	bc.enqueueCommit(job)

	// 6. Flush the scope's latest-domain rows + drop solidified buffer layers,
	//    both in the foreground (the foreground owns the scope; the worker never
	//    touches it). The cutoff is capped at block.Number()-1: this block's
	//    layer (N) is still in flight (the worker has not committed it yet), so
	//    its latest-domain rows are not yet flushable into it, and dropping it
	//    would orphan them. Block N-1, by contrast, is already committed (the
	//    rendezvous enqueue above only returns once the worker has received this
	//    job, i.e. after it finished and committed N-1), so its rows flush here
	//    and then its layer is dropped — FlushLatestUpTo(cutoff) ALWAYS precedes
	//    postFlush(cutoff) for the same cutoff, so FlushUpTo never drops a layer
	//    whose scope rows are still pending. The final block's rows are flushed
	//    by the range executor's scope Close (FlushLatest) at range end.
	//    (Synchronous commit flushes at the true solidified because the layer is
	//    committed in-line before this point; async must lag by one.)
	cutoff := dynProps.LatestSolidifiedBlockNum()
	// Cap the flush at the highest block whose buffer layer is already committed.
	// At depth 2 that is block.Number()-1: the rendezvous enqueue only returned
	// after the worker committed N-1. At depth > 2 the worker's published head
	// (bc.CurrentBlock()) may lag the foreground by up to D blocks; only committed
	// layers are flushable, and flushing an in-flight block's latest-domain scope
	// rows would orphan them, so the cutoff must track the committed head.
	maxFlushable := int64(block.Number()) - 1
	if bc.pipelinedCommit() {
		maxFlushable = int64(bc.CurrentBlock().Number())
	}
	if cutoff > maxFlushable {
		cutoff = maxFlushable
	}
	if cutoff > 0 {
		if err := plan.FlushLatestUpTo(cutoff); err != nil {
			return err
		}
		if err := bc.postFlush(cutoff); err != nil {
			return err
		}
	}
	stats.mark(&stats.Persist)
	return nil
}

// enqueueCommit posts the pending-commit barrier and hands the job to the
// worker. The send blocks on the unbuffered queue until the worker receives it
// (depth-2 backpressure). Callers hold chainmu.
func (bc *BlockChain) enqueueCommit(job *commitJob) {
	bc.commitPending.post()
	if bc.commitClosed || bc.commitQueue == nil {
		// Worker stopped (Close in progress): run inline so the job is not lost
		// and the barrier is balanced.
		bc.runCommitJob(job)
		return
	}
	bc.commitQueue <- job
}

// runCommitJob runs the deferred fold + ordered publish tail for one block on
// the serial commit worker. It mirrors the synchronous tail of
// applyBlockWithPlan, writing through a buffer LayerView bound to the block's
// in-flight layer and consuming the captured snapshots. The first error is
// recorded fail-fast in commitErr (surfaced by the next applyBlockWithPlan and
// by switchFork/Close) and the block's in-flight layer is discarded.
//
// KEEP IN SYNC with applyBlockWithPlan's synchronous commit tail.
func (bc *BlockChain) runCommitJob(job *commitJob) {
	defer bc.commitPending.done()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		// A prior commit already failed; do not apply further state. Drop the
		// layer so it is not left dangling.
		bc.buffer.DiscardInflight(job.layer)
		return
	}

	index := bc.buffer.ViewLayer(job.layer)

	// Test seam: simulate a worker-side fold failure for a specific block, to
	// exercise the speculative-exec unwind without a real disk error. Nil in
	// production (zero cost).
	if commitFoldHook != nil {
		if err := commitFoldHook(job.block.Number()); err != nil {
			bc.failCommit(job, fmt.Errorf("async commit fold block %d: %w", job.block.Number(), err))
			return
		}
	}

	// Fold (the ~55% commit cost), producing this block's internal state root.
	root, err := job.captured.Fold(index)
	if err != nil {
		bc.failCommit(job, fmt.Errorf("async commit fold block %d: %w", job.block.Number(), err))
		return
	}
	// Commitment checkpoint + StageCommitment progress (needs the root).
	if err := job.plan.finishCommitState(index, job.block, root, job.checkpoint); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit finish state block %d: %w", job.block.Number(), err))
		return
	}

	// Derived DP keys + cycle-reward pending accumulator (captured snapshots).
	job.dynProps.Flush(index)
	if err := job.cycleRewards.Write(index); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit cycle rewards block %d: %w", job.block.Number(), err))
		return
	}

	// Out-of-band metadata batch to disk (block, state root, TAPOS, tx infos,
	// tx index) — durable BEFORE the head pointer advances, preserving the
	// head=N ⟹ root[N] durable invariant for off-lock readers.
	if err := bc.writeBlockMetadataBatch(job.block, root, job.txInfos); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit metadata block %d: %w", job.block.Number(), err))
		return
	}
	rawdb.WriteHeadBlockHash(index, job.block.Hash())

	// Publish the new head, then the DP snapshot, in that order.
	bc.currentBlock.Store(job.block)
	bc.lastInsertNano.Store(time.Now().UnixNano())
	bc.storeDynPropsCache(job.dynProps)

	// Fire maintenance hooks before block hooks so the SRL PBFT message precedes
	// the block PREPREPARE (java-tron MaintenanceManager.applyBlock ordering).
	// dynPropsCache was just set to block N's DP, so ProcessOnBlock(N) reads the
	// correct epoch (decision-(b)).
	if job.wasMaintenance && job.block.Number() != 1 {
		bc.maintHookMu.Lock()
		mhooks := bc.maintHooks
		bc.maintHookMu.Unlock()
		for _, h := range mhooks {
			h(job.block, job.maintNewWitnesses)
		}
	}
	bc.blockHookMu.Lock()
	hooks := bc.blockHooks
	bc.blockHookMu.Unlock()
	for _, h := range hooks {
		h(job.block)
	}

	if err := job.plan.pipeline.Advance(rawdb.StageFinish); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit stage finish block %d: %w", job.block.Number(), err))
		return
	}

	// Promote this block's layer onto the committed stack (FIFO; the worker
	// commits in fold order, so this is always the oldest in-flight layer).
	if err := bc.buffer.CommitInflight(job.layer); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit promote layer block %d: %w", job.block.Number(), err))
		return
	}
}

// failCommit records the first commit-worker error fail-fast and discards the
// failed block's in-flight buffer layer so it cannot be promoted or flushed.
// The error is surfaced at the next applyBlockWithPlan and by switchFork/Close;
// the foreground's error path drains the worker and rewinds.
func (bc *BlockChain) failCommit(job *commitJob, err error) {
	bc.commitErr.CompareAndSwap(nil, &err)
	bc.buffer.DiscardInflight(job.layer)
	log.Error("Async commit failed", "number", job.block.Number(), "hash", job.block.Hash(), "err", err)
}

// WaitForCommitSettled blocks until every enqueued commit job has finished
// (the worker is idle). Exported-style helper used by Close and switchFork, and
// available to tests. Safe to call off chainmu.
func (bc *BlockChain) WaitForCommitSettled() {
	bc.commitPending.wait()
}

// stopCommitWorkerLocked closes the commit channel and joins the worker.
// Callers must hold chainmu and must have drained pending commits first
// (WaitForCommitSettled), so no producer is racing a send.
func (bc *BlockChain) stopCommitWorkerLocked() {
	if bc.commitQueue != nil && !bc.commitClosed {
		close(bc.commitQueue)
		bc.commitClosed = true
	}
	bc.commitWorkerWg.Wait()
}
