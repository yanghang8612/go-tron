package core

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Async-commit pipeline depth D bounds how many blocks the exec foreground may
// run ahead of the ordered commitment scheduler. The handoff channel is
// deliberately unbuffered: the scheduler owns up to D-1 submitted jobs plus the
// foreground's just-begun layer, exactly matching SetMaxInflight(D). Keeping one
// source of queueing prevents accepted jobs plus channel capacity from exceeding
// the blockbuffer bound.
//
// D == 2 retains one submitted fold plus one executing block. D > 2 lets
// first-nibble lanes advance independently into later blocks. Enabled ops-only,
// never wire-observable.
//
// The depth is resolved ONCE at NewBlockChain (so the commit worker, started in
// the constructor, ranges a correctly-sized channel and is never orphaned by a
// later re-make). SetAsyncCommit only toggles the buffer's in-flight cap.
const (
	defaultCommitPipelineDepth = 2
	maxCommitPipelineDepth     = 16
)

var (
	asyncCommitEnabledGauge             = metrics.NewRegisteredGauge("core/async_commit/enabled", nil)
	asyncCommitDepthGauge               = metrics.NewRegisteredGauge("core/async_commit/depth", nil)
	asyncCommitEnqueueCounter           = metrics.NewRegisteredCounter("core/async_commit/enqueue", nil)
	asyncCommitBackpressureCounter      = metrics.NewRegisteredCounter("core/async_commit/backpressure", nil)
	asyncCommitBackpressureNanosCounter = metrics.NewRegisteredCounter("core/async_commit/backpressure/nanos", nil)
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
// (block, blockData, txInfos, root-to-be) or a deep snapshot taken at handoff
// (dynProps, cycleRewards) so the worker never reads foreground-mutable state
// of a LATER block — see commitAsync. blockData aliases the staged layer value;
// both the worker and buffer only read it.
type commitJob struct {
	plan              *canonicalBlockExecution
	block             *types.Block
	blockData         []byte
	captured          *state.CapturedCommit
	layer             blockbuffer.InflightHandle
	index             blockbuffer.LayerView
	dynProps          *state.DynamicProperties
	cycleRewards      cycleRewardAccumulatorSnapshot
	txInfos           []*corepb.TransactionInfo
	txInfoBatch       *transactionInfoBatch
	txInfoBatchPool   *transactionInfoBatchPool
	balanceTrace      *blockBalanceTraceData
	wasMaintenance    bool
	maintNewWitnesses []tcommon.Address

	// Async telemetry is joined without making either half wait: foreground
	// commit capture/flush and worker fold/publish mark completion under this
	// mutex, and the second side publishes one complete ApplyStats snapshot.
	telemetryMu                 sync.Mutex
	telemetry                   *applyStats
	telemetryForegroundComplete bool
	telemetryWorkerComplete     bool
	telemetryPublish            bool
	telemetryPublished          bool
	deferredStateCommit         time.Duration
	deferredDPUpdate            time.Duration
	deferredPersist             time.Duration
	deferredHooks               time.Duration
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
		asyncCommitEnabledGauge.Update(1)
		asyncCommitDepthGauge.Update(int64(bc.commitDepth))
		bc.buffer.SetMaxInflight(bc.commitDepth)
	} else {
		asyncCommitEnabledGauge.Update(0)
		asyncCommitDepthGauge.Update(0)
		bc.buffer.SetMaxInflight(1)
	}
}

// PipelinedCommitDepth returns the configured async-commit pipeline depth (>=2)
// when async commit is enabled, else 0. The sync drain reuses an InsertSession
// across contiguous chunks in every mode; depth > 2 additionally buffers the
// commit worker to overlap more than one pending commitment suffix.
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

// startCommitWorker spawns the ordered commit scheduler. Like the flush worker
// it is started once construction can no longer fail. It is idle (blocked on
// the channel) until async commit is enabled and a job is enqueued, so it costs
// nothing when async commit is off. The first job uses the established serial
// fold for branch-state validation/repair; later jobs may overlap by root lane.
func (bc *BlockChain) startCommitWorker() {
	bc.commitWorkerWg.Add(1)
	go func() {
		defer bc.commitWorkerWg.Done()
		var pipeline *state.OrderedCommitmentPipeline
		pipelineEpoch := bc.commitPipelineEpoch.Load()
		defer func() {
			if pipeline != nil {
				pipeline.Close()
			}
		}()
		initializePipeline := func(repair state.CommitmentSnapshotRepair) error {
			next, err := state.NewOrderedCommitmentPipelineWithRepair(bc.buffer, repair)
			if err != nil {
				return err
			}
			pipeline = next
			return nil
		}
		bootstrapPipeline := func(job *commitJob) {
			repair := job.captured.Repair()
			bc.runCommitJob(job)
			if bc.commitErr.Load() != nil {
				return
			}
			if err := initializePipeline(repair); err != nil {
				log.Warn("Ordered commitment pipeline unavailable; using serial folds", "err", err)
			}
		}

		queue := bc.commitQueue
		pending := make([]pendingOrderedCommitJob, 0, bc.commitDepth)
		pipelineDisabled := !bc.orderedCommitPipeline
		var submissionErr error
		for queue != nil || len(pending) > 0 {
			// Bootstrap and validate the persisted branch state through the ordinary
			// fold once. That retains the established snapshot-repair/rebuild path;
			// every later block can use the persistent cross-block lane owners.
			if pipeline == nil && !pipelineDisabled {
				if queue == nil {
					break
				}
				job, ok := <-queue
				if !ok {
					queue = nil
					continue
				}
				pipelineEpoch = bc.commitPipelineEpoch.Load()
				bootstrapPipeline(job)
				if bc.commitErr.Load() != nil {
					pipelineDisabled = true
					continue
				}
				if pipeline == nil {
					// Safe fallback: ordinary serialized folds retain the exact previous
					// behavior. This is operational acceleration, never correctness.
					pipelineDisabled = true
				}
				continue
			}
			if pipelineDisabled {
				if queue == nil {
					break
				}
				job, ok := <-queue
				if !ok {
					queue = nil
					continue
				}
				bc.runCommitJob(job)
				continue
			}

			var front <-chan state.OrderedCommitmentResult
			if len(pending) > 0 {
				front = pending[0].result
			}
			receiveQueue := queue
			if len(pending) >= bc.commitDepth-1 {
				receiveQueue = nil
			}
			select {
			case job, ok := <-receiveQueue:
				if !ok {
					queue = nil
					continue
				}
				currentEpoch := bc.commitPipelineEpoch.Load()
				if currentEpoch != pipelineEpoch {
					// A canonical rewind is allowed only after WaitForCommitSettled,
					// therefore no result can still depend on the old lane roots here.
					pipeline.Close()
					pipeline = nil
					pipelineEpoch = currentEpoch
					if err := initializePipeline(job.captured.Repair()); err != nil {
						// A normal fork rewind retains an exact LCA root/branch pair and
						// initializes directly. If an offline reset removed branch state,
						// run this first new-branch block through the established repair
						// path, then seed lanes from its verified result.
						log.Warn("Ordered commitment pipeline reseed requires serial repair", "err", err)
						bootstrapPipeline(job)
						if bc.commitErr.Load() != nil || pipeline == nil {
							pipelineDisabled = true
						}
						continue
					}
				}
				foldStarted := time.Now()
				var result <-chan state.OrderedCommitmentResult
				if submissionErr != nil {
					result = completedOrderedCommitmentResult(submissionErr)
				} else if commitFoldHook != nil {
					if err := commitFoldHook(job.block.Number()); err != nil {
						submissionErr = err
						result = completedOrderedCommitmentResult(submissionErr)
					}
				}
				if result == nil {
					result = job.captured.SubmitOrdered(pipeline, &job.index)
				}
				pending = append(pending, pendingOrderedCommitJob{job: job, result: result, foldStarted: foldStarted})
			case result := <-front:
				entry := pending[0]
				copy(pending, pending[1:])
				pending[len(pending)-1] = pendingOrderedCommitJob{}
				pending = pending[:len(pending)-1]
				entry.job.deferredStateCommit += time.Since(entry.foldStarted)
				bc.finishCommitJob(entry.job, result.Root, result.Err)
			}
		}
	}()
}

type pendingOrderedCommitJob struct {
	job         *commitJob
	result      <-chan state.OrderedCommitmentResult
	foldStarted time.Time
}

func completedOrderedCommitmentResult(err error) <-chan state.OrderedCommitmentResult {
	done := make(chan state.OrderedCommitmentResult, 1)
	done <- state.OrderedCommitmentResult{Err: err}
	close(done)
	return done
}

// invalidateOrderedCommitPipeline marks a canonical-state discontinuity. The
// caller first drains pending commits, then mutates/rewinds the blockbuffer. The
// scheduler revalidates and re-seeds its persistent lane roots before accepting
// the next job; ordinary forward flushes do not need invalidation because they
// preserve the same logical commitment state.
func (bc *BlockChain) invalidateOrderedCommitPipeline() {
	bc.commitPipelineEpoch.Add(1)
}

// commitAsync is the async-commit foreground half, invoked from
// applyBlockWithPlan once every shared step (exec, maintenance, rooted-DP flush,
// TAPOS/tx-count) has run. It writes the latest-domain rows into the in-memory
// scope and captures the commitment-fold inputs WITHOUT folding, snapshots the
// foreground-mutable state the publish tail will consume, hands the job to the
// ordered commit scheduler, and runs the (scope-owned, solidified-lagging)
// latest + buffer-layer flushes itself. Returns once the scheduler accepts the
// job; it produces roots concurrently by lane but advances heads, fires hooks,
// and commits layers strictly in block order.
//
// Callers hold chainmu.
func (bc *BlockChain) commitAsync(
	block *types.Block,
	blockData []byte,
	plan *canonicalBlockExecution,
	statedb *state.StateDB,
	dynProps *state.DynamicProperties,
	stats *applyStats,
	commitOpts state.CommitOptions,
	wasMaintenanceBlock bool,
	maintNewWitnesses []tcommon.Address,
	rewardAcctAddrs []tcommon.Address,
	txInfos []*corepb.TransactionInfo,
	balanceTrace *blockBalanceTraceData,
) (retErr error) {
	// 1. Write latest-domain rows to the scope + capture the fold inputs. On the
	//    deep pipeline (depth>2) tag the scoped latest writer so its prunePending
	//    verifies durability before dropping read-your-writes overlay entries — the
	//    guard against a lost write if an entry's block tag ever diverges from the
	//    buffer layer its op bound to. Depth 2 keeps the fast byte-identical prune.
	commitOpts.DeepAsync = bc.pipelinedCommit()
	commitStats, err := plan.CommitStateCapture(block, commitOpts)
	if err != nil {
		return err
	}
	captured := statedb.TakeCapturedFold()
	if captured == nil {
		return fmt.Errorf("async commit: deferred commit produced no captured fold")
	}
	capturedHandedOff := false
	defer func() {
		if !capturedHandedOff {
			captured.Release()
		}
	}()
	stats.StateCommitDetail = commitStats
	stats.mark(&stats.StateCommit)

	// 2. Refresh the system/reward account caches FOREGROUND, reading the
	//    StateDB as of THIS block before the next block mutates the reused
	//    StateDB. (The synchronous path does this after the fold; the fold does
	//    not change account values, so reading here is value-identical.)
	bc.updateSystemAccountCache(statedb)
	bc.updateRewardAccountCache(statedb, rewardAcctAddrs)

	// 3. Capture the in-flight layer this block owns. The job embeds its bound
	//    view so the stage pipeline and worker fold can share it without two
	//    separately allocated LayerView wrappers.
	hN, ok := bc.buffer.NewestInflight()
	if !ok {
		return fmt.Errorf("async commit: no in-flight buffer layer to commit")
	}

	// 4. Snapshot the foreground-mutable in-memory state the publish tail reads,
	//    so the worker never observes a LATER block's value:
	//      - dynProps: a Copy (decision-(b)); the worker also publishes it to
	//        dynPropsCache, so ProcessOnBlock(N) reads block N's DP.
	//      - cycleRewards: a deep snapshot of the pending accumulator.
	//    block / blockData / root / txInfos are immutable.
	job := &commitJob{
		plan:              plan,
		block:             block,
		blockData:         blockData,
		captured:          captured,
		layer:             hN,
		dynProps:          bc.copyDynPropsForCommit(dynProps),
		cycleRewards:      bc.cycleRewards.Snapshot(),
		txInfos:           txInfos,
		balanceTrace:      balanceTrace,
		wasMaintenance:    wasMaintenanceBlock,
		maintNewWitnesses: maintNewWitnesses,
		telemetry:         stats,
	}
	defer func() {
		bc.completeAsyncApplyTelemetry(job, false, retErr == nil)
	}()
	job.txInfoBatch = plan.txInfoBatch
	job.txInfoBatchPool = plan.txInfoBatchPool
	bc.buffer.ViewLayerInto(hN, &job.index)
	// The worker's post-execution stage advances (StageCommitment, StageFinish)
	// must land in THIS block's layer rather than whatever layer is newest then.
	plan.pipeline.SetWriter(&job.index)

	// 5. Hand the fold + publish tail to the ordered commit scheduler. The
	//    unbuffered handoff has no hidden capacity; the scheduler owns at most
	//    depth-1 jobs. After this returns the foreground may begin the next
	//    block's layer.
	plan.txInfoBatchHandedOff = true
	capturedHandedOff = true
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
	// Cap the flush at the highest block whose buffer layer is already COMMITTED.
	// At depth 2 that is block.Number()-1: the rendezvous enqueue only returned
	// after the worker fully committed N-1 (CommitInflight done). At depth > 2 the
	// worker publishes bc.CurrentBlock() BEFORE CommitInflight, so the published
	// head's layer can still be in-flight; FlushLatestUpTo KEEPS ops targeting an
	// in-flight layer (writeFiltered only applies committed targets), so a cutoff
	// of currentBlock would leave the head block's latest-domain op queued while a
	// later postFlush drops its (by-then committed) layer → "batch target layer is
	// no longer pending". Track the newest COMMITTED layer instead — any op ≤ it
	// targets a promoted layer and is flushed (not kept) before its layer is
	// dropped. No committed layer yet ⇒ nothing is flushable this round.
	maxFlushable := int64(block.Number()) - 1
	if bc.pipelinedCommit() {
		if n, ok := bc.buffer.NewestCommittedNumber(); ok {
			maxFlushable = int64(n)
		} else {
			maxFlushable = 0
		}
		// CommitInflight and archiveHead publication are separate atomic steps.
		// Never let the durable base advance through the tiny interval between
		// them, otherwise a reader pinned to the previous archive head could see
		// future flat-latest rows from Pebble.
		if head := bc.archiveReadableHead(); head != nil && int64(head.Number()) < maxFlushable {
			maxFlushable = int64(head.Number())
		}
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

// completeAsyncApplyTelemetry joins the foreground and worker portions of one
// block's timing without adding a dependency edge to the commit pipeline. The
// caller that arrives second publishes; inline-worker fallback is therefore
// safe as well as the normal concurrent path.
func (bc *BlockChain) completeAsyncApplyTelemetry(job *commitJob, worker, publish bool) {
	if job == nil || job.telemetry == nil {
		return
	}
	job.telemetryMu.Lock()
	if worker {
		job.telemetryWorkerComplete = true
	} else {
		job.telemetryForegroundComplete = true
		job.telemetryPublish = publish
	}
	ready := job.telemetryForegroundComplete && job.telemetryWorkerComplete && job.telemetryPublish && !job.telemetryPublished
	var snapshot ApplyStats
	if ready {
		snapshot = job.telemetry.ApplyStats
		snapshot.StateCommit += job.deferredStateCommit
		snapshot.DPUpdate += job.deferredDPUpdate
		snapshot.Persist += job.deferredPersist
		snapshot.Hooks += job.deferredHooks
		job.telemetryPublished = true
	}
	job.telemetryMu.Unlock()
	if ready {
		bc.publishApplyStats(job.block, snapshot)
	}
}

// enqueueCommit posts the pending-commit barrier and hands the job to the
// scheduler. The send blocks on the unbuffered queue until the scheduler owns
// the job; its depth-1 pending bound provides backpressure. Callers hold
// chainmu.
func (bc *BlockChain) enqueueCommit(job *commitJob) {
	bc.commitPending.post()
	if bc.commitClosed || bc.commitQueue == nil {
		// Worker stopped (Close in progress): run inline so the job is not lost
		// and the barrier is balanced.
		bc.runCommitJob(job)
		return
	}
	asyncCommitEnqueueCounter.Inc(1)
	select {
	case bc.commitQueue <- job:
		return
	default:
	}
	asyncCommitBackpressureCounter.Inc(1)
	start := time.Now()
	bc.commitQueue <- job
	asyncCommitBackpressureNanosCounter.Inc(time.Since(start).Nanoseconds())
}

// runCommitJob runs the deferred fold + ordered publish tail for one block on
// the serial fallback path. It mirrors the synchronous tail of
// applyBlockWithPlan, writing through a buffer LayerView bound to the block's
// in-flight layer and consuming the captured snapshots. The first error is
// recorded fail-fast in commitErr (surfaced by the next applyBlockWithPlan and
// by switchFork/Close) and the block's in-flight layer is discarded.
//
// KEEP IN SYNC with applyBlockWithPlan's synchronous commit tail.
func (bc *BlockChain) runCommitJob(job *commitJob) {
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		bc.discardCommitJob(job)
		return
	}

	index := &job.index

	foldStarted := time.Now()
	// Test seam: simulate a worker-side fold failure for a specific block, to
	// exercise the speculative-exec unwind without a real disk error. Nil in
	// production (zero cost).
	if commitFoldHook != nil {
		if err := commitFoldHook(job.block.Number()); err != nil {
			bc.finishCommitJob(job, tcommon.Hash{}, err)
			return
		}
	}

	// Fold (the ~55% commit cost), producing this block's internal state root.
	root, err := job.captured.Fold(index)
	job.deferredStateCommit += time.Since(foldStarted)
	bc.finishCommitJob(job, root, err)
}

func (bc *BlockChain) discardCommitJob(job *commitJob) {
	bc.commitPending.done()
	job.txInfoBatchPool.release(job.txInfoBatch)
	job.captured.Release()
	bc.buffer.DiscardInflight(job.layer)
}

func (bc *BlockChain) finishCommitJob(job *commitJob, root tcommon.Hash, foldErr error) {
	defer bc.commitPending.done()
	defer job.txInfoBatchPool.release(job.txInfoBatch)
	defer job.captured.Release()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		bc.buffer.DiscardInflight(job.layer)
		return
	}
	if foldErr != nil {
		bc.failCommit(job, fmt.Errorf("async commit fold block %d: %w", job.block.Number(), foldErr))
		return
	}

	index := &job.index
	// Publish StageCommitment only after the fold succeeds.
	phaseStarted := time.Now()
	if err := job.plan.finishCommitState(); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit finish state block %d: %w", job.block.Number(), err))
		return
	}
	job.deferredStateCommit += time.Since(phaseStarted)

	// Derived DP keys + cycle-reward pending accumulator (captured snapshots).
	phaseStarted = time.Now()
	job.dynProps.Flush(index)
	if err := job.cycleRewards.Write(index); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit cycle rewards block %d: %w", job.block.Number(), err))
		return
	}
	job.deferredDPUpdate += time.Since(phaseStarted)

	// Out-of-band metadata batch to disk (block, state root, TAPOS, per-block
	// tx infos, and normally tx lookup) — durable BEFORE the head pointer advances, preserving the
	// head=N ⟹ root[N] durable invariant for off-lock readers.
	phaseStarted = time.Now()
	if err := bc.writeBlockMetadataBatch(job.block, job.blockData, root, job.txInfos, job.balanceTrace, !job.plan.deferTransactionLookup); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit metadata block %d: %w", job.block.Number(), err))
		return
	}
	// Keep the body/TAPOS rows in this layer for foreground reads and potential
	// rewind, while skipping their duplicate write when the committed layer is
	// eventually flushed to Pebble.
	if err := bc.buffer.MarkInflightWritesDurable(job.layer, blockMetadataOverlayKeys(job.block)...); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit mark durable metadata block %d: %w", job.block.Number(), err))
		return
	}
	rawdb.WriteHeadBlockHash(index, job.block.Hash())

	// Publish the new head, then the DP snapshot, in that order.
	bc.currentBlock.Store(job.block)
	bc.lastInsertNano.Store(time.Now().UnixNano())
	bc.storeReusableDynPropsCache(job.dynProps)
	job.deferredPersist += time.Since(phaseStarted)

	// Fire maintenance hooks before block hooks so the SRL PBFT message precedes
	// the block PREPREPARE (java-tron MaintenanceManager.applyBlock ordering).
	// dynPropsCache was just set to block N's DP, so ProcessOnBlock(N) reads the
	// correct epoch (decision-(b)).
	phaseStarted = time.Now()
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
	job.deferredHooks += time.Since(phaseStarted)

	phaseStarted = time.Now()
	if err := job.plan.pipeline.Advance(rawdb.StageFinish); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit stage finish block %d: %w", job.block.Number(), err))
		return
	}
	if err := job.plan.AdvanceTransactionLookupStage(index, job.block); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit tx lookup stage block %d: %w", job.block.Number(), err))
		return
	}
	if bc.config != nil && bc.config.HistoryEnabled {
		if err := job.plan.AdvanceStateHistoryIndexStage(bc.buffer, index, job.block); err != nil {
			bc.failCommit(job, fmt.Errorf("async commit state history index stage block %d: %w", job.block.Number(), err))
			return
		}
	}

	// Promote this block's layer onto the committed stack (FIFO; the worker
	// commits in fold order, so this is always the oldest in-flight layer).
	if err := bc.buffer.CommitInflight(job.layer); err != nil {
		bc.failCommit(job, fmt.Errorf("async commit promote layer block %d: %w", job.block.Number(), err))
		return
	}
	bc.archiveHead.Store(job.block)
	job.deferredPersist += time.Since(phaseStarted)
	bc.completeAsyncApplyTelemetry(job, true, true)
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
