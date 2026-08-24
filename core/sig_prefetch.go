package core

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/types"
)

var (
	blockPreprocessBlocksCounter         = metrics.NewRegisteredCounter("core/block_preprocess/blocks", nil)
	blockPreprocessTransactionsCounter   = metrics.NewRegisteredCounter("core/block_preprocess/transactions", nil)
	blockPreprocessContractErrorsCounter = metrics.NewRegisteredCounter("core/block_preprocess/contract_errors", nil)
	blockPreprocessRunsCounter           = metrics.NewRegisteredCounter("core/block_preprocess/runs", nil)
	blockPreprocessWallNanosCounter      = metrics.NewRegisteredCounter("core/block_preprocess/wall_nanos", nil)
	blockPreprocessReusedRunsCounter     = metrics.NewRegisteredCounter("core/block_preprocess/reused_runs", nil)
	blockPreprocessMismatchedRunsCounter = metrics.NewRegisteredCounter("core/block_preprocess/mismatched_runs", nil)
)

// ParallelSigVerifyMinTxs gates the immutable block-preprocessing pass run at
// the top of InsertBlocks. Above the threshold, one bounded pool warms sender
// and witness recovery, transaction contract/size facts, and body Merkle roots.
// The historical variable name is retained because signature recovery remains
// the dominant work and the threshold is an internal test/benchmark control.
//
// 0 disables the parallel pre-pass entirely (pure inline recovery, the original
// behavior) — an operational kill switch, never a consensus toggle. Both paths
// reject EXACTLY the same signatures: the pre-pass only *warms* memos, it makes
// no accept/reject decision; the serial path still owns every check and observes
// an identical recovered value whether it was precomputed or computed inline.
//
// The default keeps light steady-state extensions serial while a real sync
// batch fans immutable work out across idle cores. The ordered path still owns
// every header, envelope, and state-dependent validation decision.
var ParallelSigVerifyMinTxs = defaultParallelSigVerifyMinTxs

// ParallelSigVerifyMaxWorkers caps the shared preprocessing worker pool. A
// positive value is an explicit cap; zero or less uses the automatic policy,
// which reserves one GOMAXPROCS slot for overlapping state/commitment work.
// ECDSA recovery runs in cgo and releases its P, so a GOMAXPROCS-sized recovery
// pool otherwise lets the Go scheduler start another GOMAXPROCS CPU-bound
// workers on the same host and oversubscribe it during sync.
var ParallelSigVerifyMaxWorkers int

const defaultParallelSigVerifyMinTxs = 16

// headerSignaturePrewarmer is the optional capability a consensus engine exposes
// to let the pre-pass warm a block's header-signature recovery. The DPoS engine
// implements it; engines (or test mocks) that don't are simply skipped — the
// header signer is then recovered inline during VerifyHeaderWithDynProps, with an
// identical result. Kept as a duck-typed interface so the mandatory consensus
// .Engine interface (and its test mocks) need no change.
type headerSignaturePrewarmer interface {
	PrewarmHeaderSignature(block *types.Block)
}

// sigPrewarmJobHook, when non-nil, is invoked once per recovery job executed by
// the parallel pre-pass. It is nil in production (a single branch-predicted
// nil-check, no state) and is set only by tests to assert the cache is actually
// warmed on the happy path / not touched when the kill switch is off.
var sigPrewarmJobHook func()

// SignaturePrewarmRun owns the preprocessing worker lifetime of one batch.
// Callers may execute blocks while it runs, but must Wait before releasing the
// batch. Each derived fact is protected by the transaction/block memo that the
// serial path reads, so an early consumer waits only for its own in-flight fact.
//
// The exported handle lets the sync downloader start immutable work while the
// previous batch is still importing, then hand ownership to canonical insertion
// instead of creating a duplicate worker pool for the same blocks.
type SignaturePrewarmRun struct {
	wg         sync.WaitGroup
	remaining  atomic.Int64
	wallNanos  atomic.Int64
	started    time.Time
	jobs       int
	blockCount int
	firstBlock *types.Block
	lastBlock  *types.Block
}

// signaturePrewarmJob is deliberately pointer-only. The former job shape kept
// a full transaction slice plus an index (40 bytes on 64-bit systems) for every
// transaction in a sync batch. A direct transaction pointer carries the same
// immutable work item in 16 bytes and avoids repeatedly retaining the parent
// slice header in the flattened queue.
type signaturePrewarmJob struct {
	block *types.Block
	tx    *types.Transaction
}

func (r *SignaturePrewarmRun) Wait() {
	if r != nil {
		r.wg.Wait()
	}
}

// Ready reports whether every preprocessing job has completed. A nil handle is
// ready by definition: the batch was below the parallel-preprocessing threshold.
func (r *SignaturePrewarmRun) Ready() bool {
	return r == nil || r.remaining.Load() == 0
}

// Jobs returns the number of immutable preprocessing jobs in this run.
func (r *SignaturePrewarmRun) Jobs() int {
	if r == nil {
		return 0
	}
	return r.jobs
}

// WallTime returns the elapsed preprocessing wall time. For an in-flight run it
// returns the elapsed time so far; after completion it is stable.
func (r *SignaturePrewarmRun) WallTime() time.Duration {
	if r == nil || r.started.IsZero() {
		return 0
	}
	if elapsed := r.wallNanos.Load(); elapsed != 0 {
		return time.Duration(elapsed)
	}
	return time.Since(r.started)
}

// Matches verifies that a reusable handle belongs to this exact batch. Sync
// batches are immutable and contiguous; length plus endpoint identity protects
// accidental cross-batch reuse without retaining a second block slice.
func (r *SignaturePrewarmRun) Matches(blocks []*types.Block) bool {
	if r == nil || len(blocks) != r.blockCount || len(blocks) == 0 {
		return false
	}
	return blocks[0] == r.firstBlock && blocks[len(blocks)-1] == r.lastBlock
}

// prewarmBlockSignatures is the historical synchronous wrapper retained for
// focused callers and benchmarks. Block insertion starts the same shared
// preprocessing asynchronously and joins it before releasing the batch.
func prewarmBlockSignatures(blocks []*types.Block, engine headerSignaturePrewarmer) {
	startBlockSignaturePrewarm(blocks, engine).Wait()
}

// startBlockSignaturePrewarm warms immutable preprocessing memos for a
// contiguous batch: transaction hash/signers, contract decode/wire sizes, each
// block's transaction Merkle root, and (when supported) recovered witness. It
// never makes an accept/reject decision; ordered verification consumes the same
// memoized values and errors.
//
// Concurrency safety: transaction signers/contract facts and block witness/body
// facts are each populated at most once from immutable protobuf fields. Blocks
// the pass never sees simply compute the same facts inline.
func startBlockSignaturePrewarm(blocks []*types.Block, engine headerSignaturePrewarmer) *SignaturePrewarmRun {
	if ParallelSigVerifyMinTxs <= 0 || len(blocks) == 0 {
		return nil
	}

	// Count directly from the immutable protobuf first. Besides giving the
	// flattened queue an exact capacity, this lets sub-threshold batches return
	// without constructing Transaction wrappers that the serial path may never
	// need (for example, after a header rejection).
	totalTx := 0
	blockJobs := 0
	for _, block := range blocks {
		if block == nil {
			continue
		}
		pb := block.Proto()
		if pb != nil {
			totalTx += len(pb.GetTransactions())
			blockJobs++
		}
	}
	// Gate on transaction volume: a near-empty batch is cheaper to recover
	// inline than to fan out. Block-only jobs don't count toward the gate.
	if totalTx < ParallelSigVerifyMinTxs || totalTx+blockJobs == 0 {
		return nil
	}

	// Flatten the batch into independent preprocessing jobs so work is balanced
	// across goroutines regardless of how txs are distributed between blocks.
	jobs := make([]signaturePrewarmJob, 0, totalTx+blockJobs)
	for _, block := range blocks {
		if block == nil || block.Proto() == nil {
			continue
		}
		jobs = append(jobs, signaturePrewarmJob{block: block})
		for _, tx := range block.Transactions() {
			jobs = append(jobs, signaturePrewarmJob{tx: tx})
		}
	}

	workers := signaturePrewarmWorkerCount(len(jobs))

	run := &SignaturePrewarmRun{
		started:    time.Now(),
		jobs:       len(jobs),
		blockCount: len(blocks),
		firstBlock: blocks[0],
		lastBlock:  blocks[len(blocks)-1],
	}
	run.remaining.Store(int64(len(jobs)))
	blockPreprocessRunsCounter.Inc(1)
	var next atomic.Int64
	n := int64(len(jobs))
	for w := 0; w < workers; w++ {
		run.wg.Add(1)
		go func() {
			defer run.wg.Done()
			for {
				idx := next.Add(1) - 1
				if idx >= n {
					return
				}
				runSigJob(jobs[idx], engine)
				if run.remaining.Add(-1) == 0 {
					elapsed := time.Since(run.started).Nanoseconds()
					run.wallNanos.Store(elapsed)
					blockPreprocessWallNanosCounter.Inc(elapsed)
				}
			}
		}()
	}
	return run
}

// StartSignaturePrewarm starts the same immutable preprocessing used by block
// insertion and returns its ownership handle. The caller must either pass the
// handle to a Prewarmed insertion method for the same batch or call Wait.
func (bc *BlockChain) StartSignaturePrewarm(blocks []*types.Block) *SignaturePrewarmRun {
	if bc == nil {
		return nil
	}
	return startBlockSignaturePrewarm(blocks, bc.headerSigPrewarmer())
}

// signaturePrewarmForBlocks validates an optional reusable run and falls back
// to a fresh run when necessary. A mismatched run is joined before replacement
// so its workers never escape the lifetime promised by its original caller.
func (bc *BlockChain) signaturePrewarmForBlocks(blocks []*types.Block, reusable *SignaturePrewarmRun) *SignaturePrewarmRun {
	if reusable != nil {
		if reusable.Matches(blocks) {
			blockPreprocessReusedRunsCounter.Inc(1)
			return reusable
		}
		blockPreprocessMismatchedRunsCounter.Inc(1)
		reusable.Wait()
	}
	return bc.StartSignaturePrewarm(blocks)
}

func signaturePrewarmWorkerCount(jobCount int) int {
	if jobCount <= 0 {
		return 0
	}
	workers := runtime.GOMAXPROCS(0)
	if ParallelSigVerifyMaxWorkers > 0 {
		if workers > ParallelSigVerifyMaxWorkers {
			workers = ParallelSigVerifyMaxWorkers
		}
	} else if workers > 1 {
		workers--
	}
	if workers > jobCount {
		workers = jobCount
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

// runSigJob executes one immutable preprocessing job. Errors are intentionally
// discarded here: the serial path consumes the same memo and returns them at
// the original ordered validation boundary.
func runSigJob(job signaturePrewarmJob, engine headerSignaturePrewarmer) {
	if sigPrewarmJobHook != nil {
		sigPrewarmJobHook()
	}
	if job.block != nil {
		job.block.PrewarmTransactionMerkleRoot()
		if engine != nil {
			engine.PrewarmHeaderSignature(job.block)
		}
		blockPreprocessBlocksCounter.Inc(1)
		return
	}
	_ = job.tx.SerializedSizes()
	if _, err := job.tx.DecodedContract(); err != nil {
		blockPreprocessContractErrorsCounter.Inc(1)
	}
	_, _ = job.tx.RecoverSigners()
	blockPreprocessTransactionsCounter.Inc(1)
}
