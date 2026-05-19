package core

import (
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// newAsyncFlushChainOn spins up a single-witness chain on the supplied
// store. Single-SR setup keeps `solidified == head` every block, so the
// async-flush worker has real work to do on every applyBlock.
func newAsyncFlushChainOn(t *testing.T, diskdb ethdb.Database, witnessAddr tcommon.Address) *BlockChain {
	t.Helper()
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // no maintenance during the run
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	return bc
}

func newAsyncFlushChain(t *testing.T, witnessAddr tcommon.Address) (*BlockChain, ethdb.Database) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	return newAsyncFlushChainOn(t, diskdb, witnessAddr), diskdb
}

// TestAsyncFlush_DrainsViaWorker asserts the happy path: applyBlock can
// return before the worker has finished the disk-side flush. After
// WaitForFlushSettled returns the buffer is empty and on-disk state
// reflects the writes — equivalent to what the previous synchronous
// flush guaranteed at applyBlock-return time.
func TestAsyncFlush_DrainsViaWorker(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, diskdb := newAsyncFlushChain(t, witnessAddr)

	const N = 5
	for i := 1; i <= N; i++ {
		b := buildTestBlock(bc, witnessAddr, int64(i)*3000)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	// After the worker drains, the buffer is empty.
	bc.WaitForFlushSettled()
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("after drain: buffer holds %d layers, want 0", got)
	}

	// On-disk counters reflect every applied block.
	w := rawdb.ReadWitness(diskdb, witnessAddr)
	if w == nil {
		t.Fatal("witness record missing on disk after async drain")
	}
	if got := w.TotalProduced(); got != N {
		t.Fatalf("disk TotalProduced = %d, want %d", got, N)
	}
	if got := w.LatestBlockNum(); got != N {
		t.Fatalf("disk LatestBlockNum = %d, want %d", got, N)
	}

	// Close still succeeds and is a no-op on the buffer.
	if err := bc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("post-Close: buffer holds %d layers, want 0", got)
	}
}

// TestAsyncFlush_CloseDrainsPending verifies that Close finishes any
// in-flight flushes and runs the final synchronous flush. Even if the
// worker has fallen arbitrarily far behind, Close synchronously catches
// up before returning — the load-bearing property for graceful shutdown.
//
// We exercise this property by inserting more blocks than flushQueueCap
// without explicitly draining mid-run; the per-block insert loop posts
// to the queue, and Close must catch up.
func TestAsyncFlush_CloseDrainsPending(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, diskdb := newAsyncFlushChain(t, witnessAddr)

	const N = 6
	for i := 1; i <= N; i++ {
		b := buildTestBlock(bc, witnessAddr, int64(i)*3000)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	// Close: drains the worker + runs the final synchronous flush. After
	// Close returns the buffer is empty and the on-disk image is in sync.
	if err := bc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("post-Close: buffer holds %d layers, want 0", got)
	}

	// Disk-side counters match every applied block.
	w := rawdb.ReadWitness(diskdb, witnessAddr)
	if w == nil {
		t.Fatal("witness record missing on disk after Close")
	}
	if got := w.TotalProduced(); got != N {
		t.Fatalf("disk TotalProduced = %d, want %d", got, N)
	}
}

// TestAsyncFlush_InlineFallbackOnQueueFull pins the backpressure
// guarantee: when the channel buffer is full, postFlush takes the
// synchronous path so a flush is never lost.
//
// Reliably observing the fallback requires the worker to be deterministi-
// cally NOT draining the channel during the test. We arrange that by:
//
//  1. Stopping the chain's worker goroutine (close the channel, wait for
//     it to exit). After this, the channel is nil-able and we can
//     install a fresh full one.
//  2. Allocating a new channel and filling it to flushQueueCap — those
//     entries will never be consumed.
//  3. Calling postFlush directly. The non-blocking send must fail
//     (channel full, no consumer), select hits `default`, and the
//     inline path runs.
//
// We verify the inline path ran by injecting a sentinel error into
// flushErr just before the call: runFlushCutoff's CompareAndSwap leaves
// the existing error in place, but the inline branch in postFlush
// returns the loaded error to the caller — only the inline branch does
// this. The async-send branch returns nil unconditionally.
func TestAsyncFlush_InlineFallbackOnQueueFull(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, _ := newAsyncFlushChain(t, witnessAddr)

	// Drive one real block to advance solidified past 0 and to have
	// something on disk.
	b1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(b1); err != nil {
		t.Fatalf("setup block 1: %v", err)
	}
	bc.WaitForFlushSettled()

	// Stop the worker so it stops competing for channel slots.
	bc.chainmu.Lock()
	close(bc.flushQueue)
	bc.chainmu.Unlock()
	bc.flushWorkerWg.Wait()

	// Install a fresh channel and saturate it. The worker never starts
	// again for this BlockChain — we are explicitly emulating a stuck-
	// worker condition to force the inline path.
	bc.chainmu.Lock()
	bc.flushQueue = make(chan uint64, flushQueueCap)
	for i := 0; i < flushQueueCap; i++ {
		bc.flushPending.post()
		bc.flushQueue <- uint64(b1.Number())
	}

	// Inject a sentinel error so the inline-path return-from-flushErr
	// behaviour is observable. The async path returns nil; the inline
	// path returns the loaded error.
	sentinel := errors.New("inline-path sentinel")
	bc.flushErr.Store(&sentinel)
	bc.chainmu.Unlock()

	// postFlush MUST hit the default branch and return the sentinel.
	err := bc.postFlush(int64(b1.Number()))
	if err == nil {
		t.Fatal("postFlush returned nil; expected sentinel from inline path")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("postFlush returned unexpected error: %v", err)
	}

	// Cleanup: drain the now-saturated pre-loaded entries directly and
	// balance the WaitGroup once per entry. We can't use Close (it would
	// try to close an already-orphaned channel and re-join a dead worker).
	bc.chainmu.Lock()
	for len(bc.flushQueue) > 0 {
		<-bc.flushQueue
		bc.flushPending.done()
	}
	bc.flushQueue = nil
	bc.flushErr.Store(nil)
	bc.chainmu.Unlock()
	// No Close() call: the channel is already nil and the worker has
	// already exited. The chain is at end-of-life for this test.
}

// TestAsyncFlush_NoDoubleFlushCrash repeatedly posts the same cutoff to
// prove the worker tolerates a no-op same-cutoff post. FlushUpTo is
// idempotent on the buffer side; the worker side must not crash on the
// WaitGroup accounting.
func TestAsyncFlush_NoDoubleFlushCrash(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, _ := newAsyncFlushChain(t, witnessAddr)

	for i := 1; i <= 3; i++ {
		b := buildTestBlock(bc, witnessAddr, int64(i)*3000)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}
	bc.WaitForFlushSettled()

	// Now explicitly re-post the same cutoff several times. The buffer
	// has no layers left, FlushUpTo is a no-op, but the WaitGroup
	// accounting still needs to balance.
	for i := 0; i < 4; i++ {
		if err := bc.postFlush(3); err != nil {
			t.Fatalf("repeat postFlush #%d: %v", i, err)
		}
	}
	bc.WaitForFlushSettled()

	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("after repeated postFlush: buffer holds %d layers, want 0", got)
	}

	if err := bc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestAsyncFlush_FailFastOnNextApplyBlock pins the error-surfacing
// behaviour: if a flush returns an error, the next applyBlock surfaces
// it rather than silently continuing. We inject the error by storing
// it into flushErr directly — this is what runFlushCutoff does on a
// real failure, and isolating the test from real I/O failures keeps it
// hermetic.
func TestAsyncFlush_FailFastOnNextApplyBlock(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, _ := newAsyncFlushChain(t, witnessAddr)

	// Drive one block so the chain is in a steady state.
	b1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(b1); err != nil {
		t.Fatalf("setup block 1: %v", err)
	}
	bc.WaitForFlushSettled()

	// Simulate a worker-recorded error.
	injected := errors.New("simulated flush failure")
	bc.flushErr.Store(&injected)

	// Next InsertBlock must surface the error before doing any real work.
	b2 := buildTestBlock(bc, witnessAddr, 6000)
	err := bc.InsertBlock(b2)
	if err == nil {
		t.Fatal("expected fail-fast error on next InsertBlock, got nil")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("expected error to wrap injected failure, got %v", err)
	}

	// Close also surfaces the error.
	if err := bc.Close(); err == nil {
		t.Fatal("Close should surface async flush error")
	}
}

// TestAsyncFlush_RunFlushCutoffPreservesExistingError verifies that
// runFlushCutoff's CompareAndSwap-only error recording does not clobber
// an already-recorded error when a later flush succeeds — the first
// failure stays the actionable signal for operators.
func TestAsyncFlush_RunFlushCutoffPreservesExistingError(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, _ := newAsyncFlushChain(t, witnessAddr)

	injected := errors.New("first failure")
	bc.flushErr.Store(&injected)

	// Run a successful flush — runFlushCutoff should not displace the
	// recorded error.
	bc.flushPending.post()
	bc.runFlushCutoff(0) // cutoff 0 → flushBufferUpToSolidified returns nil
	bc.WaitForFlushSettled()

	if errPtr := bc.flushErr.Load(); errPtr == nil || !errors.Is(*errPtr, injected) {
		t.Fatalf("flushErr displaced by successful flush: got %v", errPtr)
	}

	// Close surfaces the still-recorded error.
	if err := bc.Close(); err == nil {
		t.Fatal("Close should surface previously recorded error")
	}
}
