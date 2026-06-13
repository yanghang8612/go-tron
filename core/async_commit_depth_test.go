package core

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// TestResolveCommitPipelineDepth pins the env parsing + clamp for the ops-only
// GTRON_ASYNC_COMMIT_DEPTH knob: unset/garbage/too-small → the default (2),
// too-large → the cap (16), valid → itself.
func TestResolveCommitPipelineDepth(t *testing.T) {
	cases := []struct {
		env  string
		set  bool
		want int
	}{
		{"", false, 2},
		{"2", true, 2},
		{"3", true, 3},
		{"4", true, 4},
		{"16", true, 16},
		{"1", true, 2},
		{"0", true, 2},
		{"99", true, 16},
		{"-5", true, 2},
		{"garbage", true, 2},
	}
	for _, c := range cases {
		if c.set {
			t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", c.env)
		} else {
			os.Unsetenv("GTRON_ASYNC_COMMIT_DEPTH")
		}
		if got := resolveCommitPipelineDepth(); got != c.want {
			t.Errorf("env=%q set=%v: got %d want %d", c.env, c.set, got, c.want)
		}
	}
}

// TestSetAsyncCommitDepthSizing verifies the depth resolved at construction sizes
// the commit queue (cap = D-2, fixed at NewBlockChain so the already-started
// worker never ranges a stale channel) and that SetAsyncCommit(true) raises the
// buffer's in-flight cap to D. Depth 2 == today (cap 0, maxInflight 2).
func TestSetAsyncCommitDepthSizing(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	for _, tc := range []struct {
		depth        int
		wantInflight int
		wantCap      int
	}{
		{2, 2, 0},
		{3, 3, 1},
		{4, 4, 2},
		{6, 6, 4},
	} {
		t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", fmt.Sprint(tc.depth))
		diskdb := ethrawdb.NewMemoryDatabase()
		bc := newAsyncFlushChainOn(t, diskdb, witnessAddr)
		bc.SetAsyncCommit(true)
		if got := bc.buffer.MaxInflight(); got != tc.wantInflight {
			t.Errorf("depth %d: maxInflight = %d, want %d", tc.depth, got, tc.wantInflight)
		}
		if got := cap(bc.commitQueue); got != tc.wantCap {
			t.Errorf("depth %d: commitQueue cap = %d, want %d", tc.depth, got, tc.wantCap)
		}
		if got := bc.PipelinedCommitDepth(); got != tc.depth {
			t.Errorf("depth %d: PipelinedCommitDepth = %d, want %d", tc.depth, got, tc.depth)
		}
		wantDeep := tc.depth > 2
		if got := bc.pipelinedCommit(); got != wantDeep {
			t.Errorf("depth %d: pipelinedCommit = %v, want %v", tc.depth, got, wantDeep)
		}
		bc.Close()
	}
}

// TestAsyncCommit_Depth4_MatchesSync is the deep-pipeline parity test: a single
// InsertBlocks range at depth 4 (buffered queue cap 2, maxInflight 4, generalized
// flush cutoff) must produce byte-identical per-block roots + head vs the
// synchronous reference. Exercises the buffered queue + generalized cutoff
// (cross-batch session not involved — single range).
func TestAsyncCommit_Depth4_MatchesSync(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 16
	blocks, syncRoots := buildSyncBlockSequence(t, witnessAddr, N)

	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
	diskdb := ethrawdb.NewMemoryDatabase()
	bc := newAsyncFlushChainOn(t, diskdb, witnessAddr)
	bc.SetAsyncCommit(true)
	defer bc.Close()
	if got := bc.PipelinedCommitDepth(); got != 4 {
		t.Fatalf("PipelinedCommitDepth = %d, want 4", got)
	}

	if err := bc.InsertBlocks(blocks); err != nil {
		t.Fatalf("async InsertBlocks (depth 4): %v", err)
	}
	bc.WaitForCommitSettled()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit recorded error: %v", *errPtr)
	}
	for i, b := range blocks {
		asyncRoot := rawdb.ReadBlockStateRoot(bc.chaindb, b.Hash())
		if asyncRoot != syncRoots[i] {
			t.Fatalf("block %d root mismatch: async %x != sync %x", b.Number(), asyncRoot, syncRoots[i])
		}
		if asyncRoot == (tcommon.Hash{}) {
			t.Fatalf("block %d async root is zero", b.Number())
		}
	}
	if got := bc.CurrentBlock().Hash(); got != blocks[N-1].Hash() {
		t.Fatalf("async head = %x, want %x", got, blocks[N-1].Hash())
	}
}

// TestInsertSession_CrossBatch_MatchesSync is the barrier-amortization parity
// test: a deep (depth 4) InsertSession spanning TWO batches — with NO drain
// between them — must produce byte-identical per-block roots + head vs sync. The
// session reuses one executor across the batch split, threading tip/lastDynProps/
// scope so the second batch's first block never reads a stale dynPropsCache.
func TestInsertSession_CrossBatch_MatchesSync(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 20
	blocks, syncRoots := buildSyncBlockSequence(t, witnessAddr, N)

	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
	diskdb := ethrawdb.NewMemoryDatabase()
	bc := newAsyncFlushChainOn(t, diskdb, witnessAddr)
	bc.SetAsyncCommit(true)
	defer bc.Close()

	s := bc.BeginInsertSession()
	const split = 11
	if err := s.Insert(blocks[:split]); err != nil {
		t.Fatalf("session batch 1: %v", err)
	}
	if err := s.Insert(blocks[split:]); err != nil {
		t.Fatalf("session batch 2: %v", err)
	}
	if err := s.Finish(); err != nil {
		t.Fatalf("session finish: %v", err)
	}
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit recorded error: %v", *errPtr)
	}

	for i, b := range blocks {
		got := rawdb.ReadBlockStateRoot(bc.chaindb, b.Hash())
		if got != syncRoots[i] {
			t.Fatalf("block %d root mismatch: session %x != sync %x", b.Number(), got, syncRoots[i])
		}
		if got == (tcommon.Hash{}) {
			t.Fatalf("block %d session root is zero", b.Number())
		}
	}
	if got := bc.CurrentBlock().Hash(); got != blocks[N-1].Hash() {
		t.Fatalf("session head = %x, want %x", got, blocks[N-1].Hash())
	}
}

// TestInsertSession_MaintenanceCrossingAcrossBatches is the cross-batch
// decision-(b) discriminator: a maintenance boundary (where dynamic properties
// genuinely change) falls inside the SECOND batch of a deep session, so the
// session's carried lastDynProps — not a drained dynPropsCache — must feed the
// post-boundary block. A regression that dropped the cross-batch DP carry would
// diverge a root here.
func TestInsertSession_MaintenanceCrossingAcrossBatches(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const interval = int64(12_000) // boundary every 4 blocks
	const N = 16

	// Synchronous reference.
	syncBC := newMaintenanceChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr, interval)
	blocks := make([]*types.Block, 0, N)
	syncRoots := make([]tcommon.Hash, 0, N)
	for i := 1; i <= N; i++ {
		b := buildTestBlock(syncBC, witnessAddr, int64(i)*3000)
		if err := syncBC.InsertBlock(b); err != nil {
			t.Fatalf("sync block %d: %v", i, err)
		}
		blocks = append(blocks, b)
		syncRoots = append(syncRoots, rawdb.ReadBlockStateRoot(syncBC.chaindb, b.Hash()))
	}
	syncDP := syncBC.cachedDynProps()
	syncCycle, syncNextMaint := syncDP.CurrentCycleNumber(), syncDP.NextMaintenanceTime()
	_ = syncBC.Close()
	if syncNextMaint <= interval {
		t.Fatalf("test setup: no maintenance boundary crossed (next_maintenance_time=%d)", syncNextMaint)
	}

	// Deep session across two batches; boundary at ts 12k/24k/36k/48k → blocks
	// 4,8,12,16. split=6 puts boundaries in BOTH batches incl. the second.
	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
	asyncBC := newMaintenanceChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr, interval)
	asyncBC.SetAsyncCommit(true)
	defer asyncBC.Close()

	// Adversarial regime: delay every fold so the commit worker reliably lags the
	// foreground (the 5ms sleep dwarfs an empty block's in-memory exec). With the
	// worker behind at the batch split, the published dynPropsCache is STALE when
	// batch 2's first block runs — so a correct result PROVES the session threaded
	// the carried lastDynProps forward rather than reading the cache. Neuter the
	// cross-batch carry and a post-boundary root diverges here.
	SetCommitFoldHookForTest(func(uint64) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	defer SetCommitFoldHookForTest(nil)

	s := asyncBC.BeginInsertSession()
	const split = 6
	if err := s.Insert(blocks[:split]); err != nil {
		t.Fatalf("session batch 1: %v", err)
	}
	if err := s.Insert(blocks[split:]); err != nil {
		t.Fatalf("session batch 2: %v", err)
	}
	if err := s.Finish(); err != nil {
		t.Fatalf("session finish: %v", err)
	}
	if errPtr := asyncBC.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit error: %v", *errPtr)
	}

	for i, b := range blocks {
		got := rawdb.ReadBlockStateRoot(asyncBC.chaindb, b.Hash())
		if got != syncRoots[i] {
			t.Fatalf("block %d root mismatch across maintenance (cross-batch): session %x != sync %x", b.Number(), got, syncRoots[i])
		}
	}
	asyncDP := asyncBC.cachedDynProps()
	if got := asyncDP.CurrentCycleNumber(); got != syncCycle {
		t.Fatalf("session current_cycle_number = %d, want %d", got, syncCycle)
	}
	if got := asyncDP.NextMaintenanceTime(); got != syncNextMaint {
		t.Fatalf("session next_maintenance_time = %d, want %d", got, syncNextMaint)
	}
}

// TestAsyncCommit_Depth4_FoldErrorUnwind is the H6 unwind at depth 4: a worker
// fold failure at block failAt while the foreground has run up to 4 blocks ahead
// must still unwind to the synchronous outcome — head stops at failAt-1, its root
// matches sync, and no in-flight layer for a block >= failAt is left dangling.
func TestAsyncCommit_Depth4_FoldErrorUnwind(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 8
	const failAt = uint64(4)
	blocks, syncRoots := buildSyncBlockSequence(t, witnessAddr, N)

	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
	asyncBC := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	asyncBC.SetAsyncCommit(true)

	SetCommitFoldHookForTest(func(blockNum uint64) error {
		if blockNum == failAt {
			return errors.New("injected fold failure (depth 4)")
		}
		return nil
	})
	defer SetCommitFoldHookForTest(nil)

	err := asyncBC.InsertBlocks(blocks)
	if err == nil {
		t.Fatal("InsertBlocks must surface the worker fold failure at depth 4")
	}
	asyncBC.WaitForCommitSettled()

	head := asyncBC.CurrentBlock()
	if head.Number() != failAt-1 {
		t.Fatalf("head after fold failure = %d, want %d (last committed)", head.Number(), failAt-1)
	}
	for i := uint64(1); i < failAt; i++ {
		got := rawdb.ReadBlockStateRoot(asyncBC.chaindb, blocks[i-1].Hash())
		if got != syncRoots[i-1] {
			t.Fatalf("committed block %d root mismatch: async %x != sync %x", i, got, syncRoots[i-1])
		}
	}
	for _, h := range asyncBC.buffer.PendingBlocks() {
		num := rawdb.ReadBlockNumber(asyncBC.chaindb, h)
		if num != nil && *num >= failAt {
			t.Fatalf("buffer holds a dangling layer for uncommitted block %d >= failAt", *num)
		}
	}
	SetCommitFoldHookForTest(nil)
	_ = asyncBC.Close() // commitErr is sticky; don't assert Close result
}

// TestInsertSession_ReorgUnderDeepPipeline drives a fork switch through a deep
// (depth 4) cross-batch session: the losing branch is applied across two batches,
// then the heavier branch triggers switchFork INSIDE an Insert (which drains the
// commit worker, rewinds, and Reset()s the shared executor). Post-reorg head +
// per-block roots of the winner must match a fully-synchronous reference.
func TestInsertSession_ReorgUnderDeepPipeline(t *testing.T) {
	witnessAddr := testInsertAddr(1)

	ref := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	genesis := ref.genesisBlock
	_ = ref.Close()
	chainA := chainFrom(genesis, witnessAddr, 10, 0)
	chainB := chainFrom(genesis, witnessAddr, 11, 1) // heavier → eventual winner

	// Synchronous reference.
	syncBC := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	defer syncBC.Close()
	if err := syncBC.InsertBlocks(chainA); err != nil {
		t.Fatalf("sync chain A: %v", err)
	}
	if err := syncBC.InsertBlocks(chainB); err != nil {
		t.Fatalf("sync chain B: %v", err)
	}
	if syncBC.CurrentBlock().Hash() != chainB[len(chainB)-1].Hash() {
		t.Fatalf("sync did not switch to chain B")
	}
	syncRoots := make([]tcommon.Hash, len(chainB))
	for i, b := range chainB {
		syncRoots[i] = rawdb.ReadBlockStateRoot(syncBC.chaindb, b.Hash())
	}

	// Deep session: chain A across two batches, then chain B (triggers switchFork
	// inside the second session Insert).
	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
	asyncBC := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	asyncBC.SetAsyncCommit(true)
	defer asyncBC.Close()
	s := asyncBC.BeginInsertSession()
	if err := s.Insert(chainA[:5]); err != nil {
		t.Fatalf("session chain A batch 1: %v", err)
	}
	if err := s.Insert(chainA[5:]); err != nil {
		t.Fatalf("session chain A batch 2: %v", err)
	}
	if err := s.Insert(chainB); err != nil {
		t.Fatalf("session chain B (switch): %v", err)
	}
	if err := s.Finish(); err != nil {
		t.Fatalf("session finish: %v", err)
	}
	if errPtr := asyncBC.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit error during reorg: %v", *errPtr)
	}

	if asyncBC.CurrentBlock().Hash() != chainB[len(chainB)-1].Hash() {
		t.Fatalf("post-reorg head = %x, want chain B tip %x", asyncBC.CurrentBlock().Hash(), chainB[len(chainB)-1].Hash())
	}
	for i, b := range chainB {
		got := rawdb.ReadBlockStateRoot(asyncBC.chaindb, b.Hash())
		if got != syncRoots[i] {
			t.Fatalf("post-reorg block %d root mismatch: session %x != sync %x", b.Number(), got, syncRoots[i])
		}
	}
}
