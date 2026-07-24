package core

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/tronprotocol/go-tron/core/types"
)

// ParallelSigVerifyMinTxs gates the parallel signature pre-verification pass run
// at the top of InsertBlocks. When the total transaction count across the batch
// is at least this value, every transaction's sender recovery (and every block's
// witness-signature recovery) is computed concurrently on a bounded worker pool,
// warming the per-tx / per-block memos so the serial execution path reads them
// instead of doing ECDSA on the critical hot path. Below the threshold the pass
// is skipped and recovery happens inline during execution.
//
// 0 disables the parallel pre-pass entirely (pure inline recovery, the original
// behavior) — an operational kill switch, never a consensus toggle. Both paths
// reject EXACTLY the same signatures: the pre-pass only *warms* memos, it makes
// no accept/reject decision; the serial path still owns every check and observes
// an identical recovered value whether it was precomputed or computed inline.
//
// The default is a small positive threshold so a batch of a few txs (single-block
// extension in steady state) stays serial and never pays goroutine-spawn
// overhead, while a real sync batch (up to maxFetchBatch blocks, each up to
// hundreds of txs) fans the ECDSA work out across idle cores. Signature recovery
// is ~6-10% of the single-threaded sync hot path; this split moves it off the
// critical path.
var ParallelSigVerifyMinTxs = defaultParallelSigVerifyMinTxs

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

// signaturePrewarmRun owns the worker lifetime of one batch. Callers may execute
// blocks while it runs, but must Wait before releasing the batch. RecoverSigners'
// sync.Once and the header memo safely turn an early serial read into a wait for
// that one in-flight recovery; workers otherwise stay ahead on later blocks.
type signaturePrewarmRun struct {
	wg sync.WaitGroup
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

func (r *signaturePrewarmRun) Wait() {
	if r != nil {
		r.wg.Wait()
	}
}

// prewarmBlockSignatures is the synchronous wrapper retained for focused callers
// and benchmarks. The block insertion paths use startBlockSignaturePrewarm
// directly so signature recovery for later blocks overlaps current-block state
// execution, then join the returned run before the batch is released.
func prewarmBlockSignatures(blocks []*types.Block, engine headerSignaturePrewarmer) {
	startBlockSignaturePrewarm(blocks, engine).Wait()
}

// startBlockSignaturePrewarm starts warming the ECDSA-recovery memos for a
// contiguous batch of blocks: each transaction's recovered signers and (when the
// engine supports it) each block's recovered witness. It is pure cache warming
// and never aborts on a bad signature; a recovery error is captured in the memo
// and surfaced, identically, by the ordered verification/envelope path.
//
// Concurrency safety: the per-tx signers memo (sync.Once) and the per-block
// witness memo (mutex-guarded) are each populated at most once and are pure
// functions of immutable proto fields, so warming them from many goroutines races
// with nothing and yields the same value the serial path would compute. Blocks the
// pre-pass never sees (e.g. fork-replay) just miss the cache and recover inline.
func startBlockSignaturePrewarm(blocks []*types.Block, engine headerSignaturePrewarmer) *signaturePrewarmRun {
	if ParallelSigVerifyMinTxs <= 0 || len(blocks) == 0 {
		return nil
	}

	// Count directly from the immutable protobuf first. Besides giving the
	// flattened queue an exact capacity, this lets sub-threshold batches return
	// without constructing Transaction wrappers that the serial path may never
	// need (for example, after a header rejection).
	totalTx := 0
	headerJobs := 0
	for _, block := range blocks {
		if block == nil {
			continue
		}
		totalTx += len(block.Proto().GetTransactions())
		if engine != nil {
			headerJobs++
		}
	}
	// Gate on transaction volume: a near-empty batch is cheaper to recover
	// inline than to fan out. Header-only jobs don't count toward the gate.
	if totalTx < ParallelSigVerifyMinTxs || totalTx+headerJobs == 0 {
		return nil
	}

	// Flatten the batch into independent recovery jobs so work is balanced
	// across goroutines regardless of how txs are distributed between blocks.
	jobs := make([]signaturePrewarmJob, 0, totalTx+headerJobs)
	for _, block := range blocks {
		if block == nil {
			continue
		}
		if engine != nil {
			jobs = append(jobs, signaturePrewarmJob{block: block})
		}
		for _, tx := range block.Transactions() {
			jobs = append(jobs, signaturePrewarmJob{tx: tx})
		}
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(jobs) {
		workers = len(jobs)
	}
	if workers < 1 {
		workers = 1
	}
	// Single worker collapses to the serial warm — still off the execution
	// path's later read, but no goroutine churn.
	if workers == 1 {
		for i := range jobs {
			runSigJob(jobs[i], engine)
		}
		return nil
	}

	run := new(signaturePrewarmRun)
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
			}
		}()
	}
	return run
}

// runSigJob executes one recovery job. A non-nil block warms the header
// signature; otherwise tx identifies the transaction signer memo to warm.
// Errors are intentionally discarded here — they are memoized and resurfaced
// by the serial path.
func runSigJob(job signaturePrewarmJob, engine headerSignaturePrewarmer) {
	if sigPrewarmJobHook != nil {
		sigPrewarmJobHook()
	}
	if job.block != nil {
		if engine != nil {
			engine.PrewarmHeaderSignature(job.block)
		}
		return
	}
	_, _ = job.tx.RecoverSigners()
}
