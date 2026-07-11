package core

import (
	"runtime"
	"sort"
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

// sigPrewarmBlockSpan maps a consecutive range of prewarm work indexes to one
// block. Header recovery, when present, occupies the first index and the
// remaining indexes address block.Transactions() directly. This keeps the
// scheduler's auxiliary memory proportional to blocks instead of transactions.
type sigPrewarmBlockSpan struct {
	block      *types.Block
	txs        []*types.Transaction
	start, end int64
	header     bool
}

// prewarmBlockSignatures concurrently warms the ECDSA-recovery memos for a
// contiguous batch of blocks: each transaction's recovered signers and (when the
// engine supports it) each block's recovered witness. It is a pure cache-warming
// step — it returns nothing and never aborts on a bad signature; a recovery error
// is captured in the memo and surfaced, identically, by the serial verification /
// envelope-validation path when it reaches that block/tx in order.
//
// Concurrency safety: the per-tx signers memo (sync.Once) and the per-block
// witness memo (mutex-guarded) are each populated at most once and are pure
// functions of immutable proto fields, so warming them from many goroutines races
// with nothing and yields the same value the serial path would compute. Blocks the
// pre-pass never sees (e.g. fork-replay) just miss the cache and recover inline.
func prewarmBlockSignatures(blocks []*types.Block, engine headerSignaturePrewarmer) {
	if ParallelSigVerifyMinTxs <= 0 || len(blocks) == 0 {
		return
	}

	// Build one span per block rather than one heap object per transaction.
	// Sync ranges can contain many transactions; retaining a full job vector
	// duplicates their scheduling metadata and increases GC work before serial
	// execution starts. The worker resolves an atomic work index through these
	// block spans, so independent transaction recovery remains balanced.
	var (
		spans    []sigPrewarmBlockSpan
		totalTx  int
		jobCount int64
	)
	for _, block := range blocks {
		if block == nil || block.Proto() == nil {
			continue
		}
		txs := block.Transactions()
		header := engine != nil
		span := sigPrewarmBlockSpan{
			block:  block,
			txs:    txs,
			start:  jobCount,
			header: header,
		}
		if header {
			jobCount++
		}
		for _, tx := range txs {
			if tx == nil || tx.Proto() == nil {
				continue
			}
			totalTx++
		}
		jobCount += int64(len(txs))
		span.end = jobCount
		if span.start != span.end {
			spans = append(spans, span)
		}
	}
	// Gate on transaction volume: a near-empty batch is cheaper to recover
	// inline than to fan out. Header-only jobs don't count toward the gate.
	if totalTx < ParallelSigVerifyMinTxs || jobCount == 0 {
		return
	}

	workers := runtime.GOMAXPROCS(0)
	if jobCount < int64(workers) {
		workers = int(jobCount)
	}
	if workers < 1 {
		workers = 1
	}
	// Single worker collapses to the serial warm — still off the execution
	// path's later read, but no goroutine churn.
	if workers == 1 {
		for i := int64(0); i < jobCount; i++ {
			runSigPrewarmIndex(spans, i, engine)
		}
		return
	}

	var (
		wg   sync.WaitGroup
		next atomic.Int64
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx := next.Add(1) - 1
				if idx >= jobCount {
					return
				}
				runSigPrewarmIndex(spans, idx, engine)
			}
		}()
	}
	wg.Wait()
}

func runSigPrewarmIndex(spans []sigPrewarmBlockSpan, index int64, engine headerSignaturePrewarmer) {
	spanIndex := sort.Search(len(spans), func(i int) bool {
		return spans[i].end > index
	})
	if spanIndex >= len(spans) {
		return
	}
	span := spans[spanIndex]
	offset := index - span.start
	if span.header {
		if offset == 0 {
			runSigJob(span.block, nil, -1, engine)
			return
		}
		offset--
	}
	runSigJob(nil, span.txs, int(offset), engine)
}

// runSigJob executes one recovery job. txIndex < 0 means warm the block's header
// signature; otherwise warm txs[txIndex]'s signers. Errors are intentionally
// discarded here — they are memoized and resurfaced by the serial path.
func runSigJob(block *types.Block, txs []*types.Transaction, txIndex int, engine headerSignaturePrewarmer) {
	if txIndex < 0 {
		if engine != nil && block != nil {
			if sigPrewarmJobHook != nil {
				sigPrewarmJobHook()
			}
			engine.PrewarmHeaderSignature(block)
		}
		return
	}
	if txIndex >= len(txs) || txs[txIndex] == nil || txs[txIndex].Proto() == nil {
		return
	}
	if sigPrewarmJobHook != nil {
		sigPrewarmJobHook()
	}
	_, _ = txs[txIndex].RecoverSigners()
}
