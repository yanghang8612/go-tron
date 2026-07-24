package core

import (
	"errors"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// chainFrom builds a deterministic linear chain of n unsigned blocks on top of
// parent. tsOffset perturbs timestamps so two chains off the same parent get
// distinct hashes (for fork tests).
func chainFrom(parent *types.Block, witnessAddr tcommon.Address, n int, tsOffset int64) []*types.Block {
	blocks := make([]*types.Block, 0, n)
	prev := parent
	for i := 1; i <= n; i++ {
		num := int64(prev.Number()) + 1
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         num,
					Timestamp:      num*3000 + tsOffset,
					ParentHash:     prev.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		blocks = append(blocks, b)
		prev = b
	}
	return blocks
}

// buildSyncBlockSequence drives a synchronous (async-OFF) single-SR chain for n
// blocks and returns the block objects plus the per-block internal state roots.
// The block objects are deterministic functions of the chain, so they can be
// replayed verbatim into an async-ON chain for a byte-for-byte root comparison.
func buildSyncBlockSequence(t *testing.T, witnessAddr tcommon.Address, n int) ([]*types.Block, []tcommon.Hash) {
	t.Helper()
	bc, _ := newAsyncFlushChain(t, witnessAddr)
	defer bc.Close()

	blocks := make([]*types.Block, 0, n)
	roots := make([]tcommon.Hash, 0, n)
	for i := 1; i <= n; i++ {
		b := buildTestBlock(bc, witnessAddr, int64(i)*3000)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("sync block %d: %v", i, err)
		}
		blocks = append(blocks, b)
		roots = append(roots, rawdb.ReadBlockStateRoot(bc.chaindb, b.Hash()))
	}
	return blocks, roots
}

// TestAsyncCommit_SameRootAsSync is the load-bearing correctness test: an
// async-ON chain that ingests the exact same blocks as the synchronous chain
// must produce byte-identical per-block internal state roots, the same head,
// and the same solidified height. A single mismatch is a consensus divergence.
func TestAsyncCommit_SameRootAsSync(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 12
	blocks, syncRoots := buildSyncBlockSequence(t, witnessAddr, N)

	// Async-ON chain on a fresh datadir; ingest the same blocks via the range
	// path (InsertBlocks) so the pipeline actually overlaps fold(N) with
	// exec(N+1).
	diskdb := ethrawdb.NewMemoryDatabase()
	bc := newAsyncFlushChainOn(t, diskdb, witnessAddr)
	bc.SetAsyncCommit(true)
	defer bc.Close()

	if err := bc.InsertBlocks(blocks); err != nil {
		t.Fatalf("async InsertBlocks: %v", err)
	}
	bc.WaitForCommitSettled()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit recorded error: %v", *errPtr)
	}

	// Per-block root parity.
	for i, b := range blocks {
		asyncRoot := rawdb.ReadBlockStateRoot(bc.chaindb, b.Hash())
		if asyncRoot != syncRoots[i] {
			t.Fatalf("block %d root mismatch: async %x != sync %x", b.Number(), asyncRoot, syncRoots[i])
		}
		if asyncRoot == (tcommon.Hash{}) {
			t.Fatalf("block %d async root is zero", b.Number())
		}
	}

	// Head parity.
	if got := bc.CurrentBlock().Hash(); got != blocks[N-1].Hash() {
		t.Fatalf("async head = %x, want %x", got, blocks[N-1].Hash())
	}

	// The async path must also have moved through more than one in-flight layer
	// at least once (otherwise it never exercised the overlap). We can't observe
	// that directly post-hoc, but the rooted witness counters confirm every
	// block committed.
	w := readWitnessAtHead(t, bc, witnessAddr)
	if got := w.TotalProduced(); got != int64(N) {
		t.Fatalf("async TotalProduced = %d, want %d", got, N)
	}
}

// TestAsyncCommit_Deterministic runs the same blocks through two independent
// async-ON chains and requires identical per-block roots run-to-run. A
// difference would indicate an ordering race in the commit worker.
func TestAsyncCommit_Deterministic(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 10
	blocks, _ := buildSyncBlockSequence(t, witnessAddr, N)

	run := func() []tcommon.Hash {
		diskdb := ethrawdb.NewMemoryDatabase()
		bc := newAsyncFlushChainOn(t, diskdb, witnessAddr)
		bc.SetAsyncCommit(true)
		defer bc.Close()
		if err := bc.InsertBlocks(blocks); err != nil {
			t.Fatalf("async InsertBlocks: %v", err)
		}
		bc.WaitForCommitSettled()
		roots := make([]tcommon.Hash, len(blocks))
		for i, b := range blocks {
			roots[i] = rawdb.ReadBlockStateRoot(bc.chaindb, b.Hash())
		}
		return roots
	}

	a := run()
	b := run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("block %d nondeterministic: run1 %x != run2 %x", i+1, a[i], b[i])
		}
	}
}

// TestAsyncCommit_CloseDrains verifies Close drains the commit worker (and the
// flush worker behind it), leaving an empty buffer and on-disk state reflecting
// every applied block — the graceful-shutdown property.
func TestAsyncCommit_CloseDrains(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 8
	blocks, syncRoots := buildSyncBlockSequence(t, witnessAddr, N)

	diskdb := ethrawdb.NewMemoryDatabase()
	bc := newAsyncFlushChainOn(t, diskdb, witnessAddr)
	bc.SetAsyncCommit(true)
	if err := bc.InsertBlocks(blocks); err != nil {
		t.Fatalf("async InsertBlocks: %v", err)
	}
	if err := bc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("post-Close: buffer holds %d layers, want 0", got)
	}
	// Final head root parity after a full drain.
	headRoot := rawdb.ReadBlockStateRoot(bc.chaindb, blocks[N-1].Hash())
	if headRoot != syncRoots[N-1] {
		t.Fatalf("post-Close head root mismatch: async %x != sync %x", headRoot, syncRoots[N-1])
	}
	w := readWitnessAtHead(t, bc, witnessAddr)
	if got := w.TotalProduced(); got != int64(N) {
		t.Fatalf("post-Close TotalProduced = %d, want %d", got, N)
	}
}

// TestAsyncCommit_FailFastOnNextInsert pins the error-surfacing behaviour: a
// recorded commit-worker error is surfaced by the next InsertBlock rather than
// silently continuing, and by Close. Mirrors the flush worker's fail-fast.
func TestAsyncCommit_FailFastOnNextInsert(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, _ := newAsyncFlushChain(t, witnessAddr)
	bc.SetAsyncCommit(true)

	// Drive one async block so the chain is steady.
	b1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(b1); err != nil {
		t.Fatalf("setup block 1: %v", err)
	}
	bc.WaitForCommitSettled()

	// Simulate a worker-recorded commit error.
	injected := errors.New("simulated async commit failure")
	bc.commitErr.Store(&injected)

	b2 := buildTestBlock(bc, witnessAddr, 6000)
	err := bc.InsertBlock(b2)
	if err == nil {
		t.Fatal("expected fail-fast error on next InsertBlock, got nil")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("expected wrapped injected error, got %v", err)
	}

	if err := bc.Close(); err == nil {
		t.Fatal("Close should surface async commit error")
	}
}

// newMaintenanceChainOn builds a single-SR chain WITH a real maintenance
// interval, so blocks whose timestamp crosses a boundary run doMaintenance
// (cycle advance, next_maintenance_time roll, witness stats, reward settlement).
// This is what makes the dynamic properties actually CHANGE across blocks, so
// the async decision-(b) DP threading is exercised rather than vacuous.
func newMaintenanceChainOn(t *testing.T, diskdb ethdb.Database, witnessAddr tcommon.Address, interval int64) *BlockChain {
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
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
			// Stake-2.0 reward delegation, so doMaintenance advances the cycle and
			// settles cycle rewards — exercising the worker's cycleRewards
			// snapshot alongside decision-(b) DP threading.
			"change_delegation": 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	return bc
}

// TestAsyncCommit_SameRootAcrossMaintenance is the decision-(b) test: it crosses
// several maintenance boundaries (where dynamic properties genuinely change —
// current_cycle_number, next_maintenance_time, block_filled_slots, witness
// stats) and requires the async per-block roots to match the synchronous
// reference. Critically it replays the blocks in TWO InsertBlocks ranges so BOTH
// the within-range DP threading (parentDynProps) AND the cross-range path (the
// first block of a range reads the freshly-drained dynPropsCache) are exercised.
// If decision-(b) regressed (a post-boundary block reading a stale DP), the
// rooted DP would diverge and a root would mismatch.
//
// NOTE on discrimination: this test exercises the decision-(b) threading path in
// the happy run, but it is NOT a guaranteed adversarial discriminator — for
// fast (empty/small) blocks the commit worker may publish dynPropsCache before
// the foreground reads it for the next block, so a regression that removed the
// threading could still pass by winning that race. The authoritative regression
// discriminator is the live OFF-vs-ON re-sync over a real maintenance-heavy
// range (validation protocol §5.1), which this unit test cannot replace.
func TestAsyncCommit_SameRootAcrossMaintenance(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const interval = int64(12_000) // boundary every 4 blocks (ts 12k,24k,36k,48k)
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
	syncCycle := syncDP.CurrentCycleNumber()
	syncNextMaint := syncDP.NextMaintenanceTime()
	_ = syncBC.Close()
	// Confirm the run actually crossed maintenance boundaries (otherwise the test
	// is vacuous): next_maintenance_time must have advanced past the seed.
	if syncNextMaint <= interval {
		t.Fatalf("test setup: no maintenance boundary crossed (next_maintenance_time=%d)", syncNextMaint)
	}

	// Async chain: replay in two ranges so a maintenance boundary falls inside
	// the second range AND the range split exercises cross-range DP freshness.
	asyncBC := newMaintenanceChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr, interval)
	asyncBC.SetAsyncCommit(true)
	defer asyncBC.Close()
	const split = 6
	if err := asyncBC.InsertBlocks(blocks[:split]); err != nil {
		t.Fatalf("async range 1: %v", err)
	}
	if err := asyncBC.InsertBlocks(blocks[split:]); err != nil {
		t.Fatalf("async range 2: %v", err)
	}
	asyncBC.WaitForCommitSettled()
	if errPtr := asyncBC.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit error: %v", *errPtr)
	}

	for i, b := range blocks {
		got := rawdb.ReadBlockStateRoot(asyncBC.chaindb, b.Hash())
		if got != syncRoots[i] {
			t.Fatalf("block %d root mismatch across maintenance: async %x != sync %x", b.Number(), got, syncRoots[i])
		}
	}
	asyncDP := asyncBC.cachedDynProps()
	if got := asyncDP.CurrentCycleNumber(); got != syncCycle {
		t.Fatalf("async current_cycle_number = %d, want %d", got, syncCycle)
	}
	if got := asyncDP.NextMaintenanceTime(); got != syncNextMaint {
		t.Fatalf("async next_maintenance_time = %d, want %d", got, syncNextMaint)
	}
}

type failAtStageProgressStore struct {
	stage rawdb.StageID
	err   error
}

func (s failAtStageProgressStore) WriteWithHash(stage rawdb.StageID, _ uint64, _ tcommon.Hash) error {
	if stage == s.stage {
		return s.err
	}
	return nil
}

func (failAtStageProgressStore) RewindCanonicalWithHash(uint64, tcommon.Hash) error {
	return nil
}

func (failAtStageProgressStore) Read(rawdb.StageID) (rawdb.StageProgress, bool, error) {
	return rawdb.StageProgress{}, false, nil
}

// TestAsyncCommit_TransfersDynPropsAndRollsBackFailedApply guards the ownership
// optimization in decision-(b): once the worker has copied block N's finalized
// properties, block N+1 reuses the range-owned object rather than copying all
// property maps again. The second half fails after ProcessBlock has advanced
// BLOCK_FILLED_SLOTS_INDEX and requires the outer lazy snapshot to restore the
// carried parent value.
func TestAsyncCommit_TransfersDynPropsAndRollsBackFailedApply(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	bc.SetAsyncCommit(true)
	defer bc.Close()

	blocks := chainFrom(bc.CurrentBlock(), witnessAddr, 3, 0)
	executor := newCanonicalRangeExecutor(bc, true)
	defer executor.Abort()

	if err := executor.Apply(blocks[0]); err != nil {
		t.Fatalf("apply block 1: %v", err)
	}
	bc.WaitForCommitSettled()
	carried := executor.lastDynProps
	if carried == nil {
		t.Fatal("block 1 did not carry finalized dynamic properties")
	}

	if err := executor.Apply(blocks[1]); err != nil {
		t.Fatalf("apply block 2: %v", err)
	}
	bc.WaitForCommitSettled()
	if executor.lastDynProps != carried {
		t.Fatal("block 2 replaced the range-owned dynamic properties instead of transferring them")
	}

	beforeIndex := carried.BlockFilledSlotsIndex()
	plannedTxRange, err := executor.txRanges.next(blocks[2])
	if err != nil {
		t.Fatalf("plan block 3 tx range: %v", err)
	}
	txInfoBatch := executor.txInfoBatches.acquire()
	defer executor.txInfoBatches.release(txInfoBatch)
	injected := errors.New("injected execution-stage progress failure")
	plan := &canonicalBlockExecution{
		state:           executor.state,
		commit:          executor.commit,
		txRange:         plannedTxRange,
		pipeline:        &canonicalStagePipeline{progress: failAtStageProgressStore{stage: rawdb.StageExecution, err: injected}, blockNum: blocks[2].Number(), hash: blocks[2].Hash(), last: -1},
		parent:          blocks[1],
		parentDynProps:  carried,
		txInfoBatch:     txInfoBatch,
		txInfoBatchPool: executor.txInfoBatches,
	}
	if err := bc.applyBlockWithPlan(blocks[2], plan); !errors.Is(err, injected) {
		t.Fatalf("apply block 3 error = %v, want injected stage failure", err)
	}
	if got := carried.BlockFilledSlotsIndex(); got != beforeIndex {
		t.Fatalf("carried block_filled_slots_index after failed apply = %d, want %d", got, beforeIndex)
	}
}

// TestAsyncCommit_ReorgMatchesSync drives a fork switch through the async path
// (the switchFork re-apply uses the shared-commit range executor, so it commits
// asynchronously) and requires the post-reorg head + per-block roots of the
// winning branch to match a fully-synchronous reference. This exercises the
// switchFork commit-worker drain: an in-flight commit must be quiesced before
// the rewind, and the re-applied branch fully committed before switchFork
// returns.
func TestAsyncCommit_ReorgMatchesSync(t *testing.T) {
	witnessAddr := testInsertAddr(1)

	// Build chains A (10 blocks) and B (11 blocks, the eventual winner) off the
	// same genesis. Use a throwaway chain only to obtain the genesis block.
	ref := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	genesis := ref.genesisBlock
	_ = ref.Close()
	chainA := chainFrom(genesis, witnessAddr, 10, 0)
	chainB := chainFrom(genesis, witnessAddr, 11, 1) // +1 ts → distinct hashes

	// Reference: synchronous chain. Insert A, then B (triggers switch to B).
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

	// Async chain: same sequence.
	asyncBC := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	asyncBC.SetAsyncCommit(true)
	defer asyncBC.Close()
	if err := asyncBC.InsertBlocks(chainA); err != nil {
		t.Fatalf("async chain A: %v", err)
	}
	if err := asyncBC.InsertBlocks(chainB); err != nil {
		t.Fatalf("async chain B (switch): %v", err)
	}
	asyncBC.WaitForCommitSettled()
	if errPtr := asyncBC.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit error during reorg: %v", *errPtr)
	}

	if asyncBC.CurrentBlock().Hash() != chainB[len(chainB)-1].Hash() {
		t.Fatalf("async head = %x, want chain B tip %x", asyncBC.CurrentBlock().Hash(), chainB[len(chainB)-1].Hash())
	}
	for i, b := range chainB {
		got := rawdb.ReadBlockStateRoot(asyncBC.chaindb, b.Hash())
		if got != syncRoots[i] {
			t.Fatalf("post-reorg block %d root mismatch: async %x != sync %x", b.Number(), got, syncRoots[i])
		}
	}
}

// TestAsyncCommit_RealFoldErrorUnwind exercises the H6 speculative-exec unwind
// with a REAL worker fold failure (not just an injected commitErr): the commit
// worker fails the fold of block K mid-range while the foreground has run ahead.
// The chain must unwind to exactly the synchronous outcome — head stops at the
// last worker-committed block (K-1), its root matches sync, the failed block's
// in-flight layer is discarded (no dangling buffer layer), and InsertBlocks
// returns an error.
func TestAsyncCommit_RealFoldErrorUnwind(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	const N = 6
	const failAt = uint64(4)
	blocks, syncRoots := buildSyncBlockSequence(t, witnessAddr, N)

	asyncBC := newAsyncFlushChainOn(t, ethrawdb.NewMemoryDatabase(), witnessAddr)
	asyncBC.SetAsyncCommit(true)

	// Inject a worker fold failure for block `failAt`.
	SetCommitFoldHookForTest(func(blockNum uint64) error {
		if blockNum == failAt {
			return errors.New("injected fold failure")
		}
		return nil
	})
	defer SetCommitFoldHookForTest(nil)

	err := asyncBC.InsertBlocks(blocks)
	if err == nil {
		t.Fatal("InsertBlocks must surface the worker fold failure")
	}

	// Head must stop at the last successfully-committed block (K-1). The
	// rendezvous commits 1..K-1 (advancing head) before the worker receives and
	// fails block K, so this is deterministic.
	head := asyncBC.CurrentBlock()
	if head.Number() != failAt-1 {
		t.Fatalf("head after fold failure = %d, want %d (last committed before failure)", head.Number(), failAt-1)
	}
	if head.Hash() != blocks[failAt-2].Hash() {
		t.Fatalf("head hash = %x, want block %d", head.Hash(), failAt-1)
	}

	// Committed blocks' roots match the synchronous reference.
	for i := uint64(1); i < failAt; i++ {
		got := rawdb.ReadBlockStateRoot(asyncBC.chaindb, blocks[i-1].Hash())
		if got != syncRoots[i-1] {
			t.Fatalf("committed block %d root mismatch: async %x != sync %x", i, got, syncRoots[i-1])
		}
	}
	// The failed block's layer was discarded — no buffer layer for block >= failAt.
	for _, h := range asyncBC.buffer.PendingBlocks() {
		num := rawdb.ReadBlockNumber(asyncBC.chaindb, h)
		if num != nil && *num >= failAt {
			t.Fatalf("buffer holds a layer for failed/uncommitted block %d (dangling)", *num)
		}
	}

	// The recorded commit error is surfaced again by a fresh insert attempt.
	SetCommitFoldHookForTest(nil)
	if err := asyncBC.InsertBlock(buildTestBlock(asyncBC, witnessAddr, int64(N+1)*3000)); err == nil {
		t.Fatal("a fresh insert after a worker fold failure must still surface the error")
	}
	// Don't assert Close success — commitErr is intentionally sticky (fail-fast).
}

// TestAsyncCommit_HeaderVerifyUsesRangeTip is the regression for the production
// stall "insert block range index 1 block N: invalid block number". Under async
// commit the serial commit worker publishes bc.CurrentBlock() off the critical
// path, so the published head lags the executor's range-local tip by up to one
// block. Header verification must validate a block's number / parent-hash / slot
// linkage against the range-local tip (plan.parent), NOT bc.CurrentBlock() —
// otherwise the 2nd+ block of an InsertBlocks range is rejected with
// ErrInvalidBlockNumber because the worker has not yet published the previous
// block's head.
//
// The existing async tests never catch this: with trivial in-memory blocks the
// worker wins the publish race, so currentBlock stays caught up. Here we force
// the production timing by delaying the worker's fold, which makes the worker lag
// on EVERY block — exactly the regime where the real fold (~55% of commit cost)
// loses the race on mainnet/Nile and surfaced as the block-101 sync stall.
func TestAsyncCommit_HeaderVerifyUsesRangeTip(t *testing.T) {
	witnessKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)
	genesis := fixedVerifyGenesis(witnessAddr)

	// Real DPoS-signed wire blocks, re-unmarshalled cold — exactly what a peer
	// delivers during sync. header verification (the path that returns
	// "invalid block number") only runs with an engine wired, so this MUST be a
	// newVerifierChain, not the engine-less async-flush helper.
	const N = 6
	raw := produceSignedBlocks(t, genesis, witnessKey, N, func(uint64) []*types.Transaction { return nil })

	asyncBC := newVerifierChain(t, genesis)
	asyncBC.SetAsyncCommit(true)
	defer asyncBC.Close()

	// Delay every fold so the commit worker reliably lags the foreground: by the
	// time the foreground verifies block K+1's header, the worker has not yet
	// stored currentBlock=K. The 20ms sleep dwarfs the foreground's khaos push +
	// state open + header verify (all in-memory, microseconds), making the lag
	// deterministic rather than timing-dependent — the same regime the real
	// ~55%-of-commit fold produces on mainnet/Nile.
	SetCommitFoldHookForTest(func(uint64) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	defer SetCommitFoldHookForTest(nil)

	if err := asyncBC.InsertBlocks(unmarshalBatch(t, raw)); err != nil {
		t.Fatalf("InsertBlocks under a lagging commit worker: %v", err)
	}
	asyncBC.WaitForCommitSettled()
	if errPtr := asyncBC.commitErr.Load(); errPtr != nil {
		t.Fatalf("async commit recorded error: %v", *errPtr)
	}
	if got := asyncBC.CurrentBlock().Number(); got != N {
		t.Fatalf("async head = %d, want %d", got, N)
	}
}

// TestAsyncCommit_OffByDefault asserts the kill switch defaults off: a freshly
// constructed chain is synchronous (maxInflight 1) so a second BeginBlock on the
// buffer still panics — the structural guarantee behind OFF byte-identity.
func TestAsyncCommit_OffByDefault(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	bc, _ := newAsyncFlushChain(t, witnessAddr)
	defer bc.Close()
	if bc.asyncCommit {
		t.Fatal("asyncCommit must default to false")
	}
	// A synchronous chain commits each block before the next begins, so the
	// buffer is never left with an uncommitted layer between inserts.
	b1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(b1); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}
	bc.WaitForCommitSettled()
	if got := bc.buffer.PendingBlocks(); len(got) > 1 {
		t.Fatalf("sync path left %d pending layers, want <=1", len(got))
	}
}

// newBlockhashProbeChain is newAsyncFlushChainOn plus a funded contract owner
// and the allow_creation_of_contracts gate, so blocks can deploy and trigger
// TVM contracts.
func newBlockhashProbeChain(t *testing.T, witnessAddr, owner tcommon.Address) *BlockChain {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
			{Address: owner, Balance: 100_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time":       1<<62 - 1, // no maintenance during the run
			"allow_creation_of_contracts": 1,
			// Modern create semantics (constructor RETURN becomes the runtime);
			// pre-Constantinople TVM ignores the return and pattern-scans the
			// creation bytes instead. The Nile era this test replays had
			// Constantinople active.
			"allow_tvm_constantinople": 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatalf("SetupGenesisBlock: %v", err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("NewBlockChain: %v", err)
	}
	return bc
}

// makeBlockhashProbeDeployTx deploys a contract whose 9-byte runtime stores
// blockhash(block.number - 1) into storage slot 0:
//
//	PUSH1 1; NUMBER; SUB; BLOCKHASH; PUSH1 0; SSTORE; STOP
func makeBlockhashProbeDeployTx(owner tcommon.Address) *types.Transaction {
	runtime := []byte{0x60, 0x01, 0x43, 0x03, 0x40, 0x60, 0x00, 0x55, 0x00}
	creation := append([]byte{0x68}, runtime...)              // PUSH9 <runtime>
	creation = append(creation, 0x60, 0x00, 0x52)             // PUSH1 0; MSTORE
	creation = append(creation, 0x60, 0x09, 0x60, 0x17, 0xf3) // RETURN mem[23:32)
	csc := &contractpb.CreateSmartContract{
		OwnerAddress: owner.Bytes(),
		NewContract: &contractpb.SmartContract{
			OriginAddress: owner.Bytes(),
			Name:          "BlockhashProbe",
			Bytecode:      creation,
		},
	}
	param, _ := anypb.New(csc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			FeeLimit:   1_000_000_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_CreateSmartContract,
				Parameter: param,
			}},
		},
		Ret: []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}},
	})
}

func makeBlockhashProbeTriggerTx(owner, contractAddr tcommon.Address) *types.Transaction {
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	}
	param, _ := anypb.New(tsc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			FeeLimit:   1_000_000_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TriggerSmartContract,
				Parameter: param,
			}},
		},
		Ret: []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}},
	})
}

// buildTxBlock chains a block carrying txs on top of parent, mirroring
// buildTestBlock's deterministic header shape.
func buildTxBlock(parent *types.Block, witnessAddr tcommon.Address, txs ...*types.Transaction) *types.Block {
	pbTxs := make([]*corepb.Transaction, 0, len(txs))
	for _, tx := range txs {
		pbTxs = append(pbTxs, tx.Proto())
	}
	num := int64(parent.Number()) + 1
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         num,
				Timestamp:      num * 3000,
				ParentHash:     parent.Hash().Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: pbTxs,
	})
}

// TestAsyncCommit_BlockHashSeesInFlightParent replays the Nile 10,552,292
// stall mechanism (OneSwap removeOrders, SUCCESS->REVERT). Under
// GTRON_ASYNC_COMMIT=1 the parent block's b-<num> row is written only by the
// commit worker's metadata batch, so a tx in block N executing
// BLOCKHASH(N-1) raced the worker and read 0 — java-tron always serves the
// parent hash. OneSwap derives limit-order ids as
// uint22(blockhash(block.number-1) ^ tx.origin); the zero hash collapsed
// gtron's ids to uint22(origin), silently diverging the on-chain order book
// at placement and surfacing 277k blocks later when removeOrders could not
// find the canonical ids.
func TestAsyncCommit_BlockHashSeesInFlightParent(t *testing.T) {
	witnessAddr := testInsertAddr(1)
	owner := testProcessorAddr(0x7A)

	run := func(t *testing.T, async bool) {
		bc := newBlockhashProbeChain(t, witnessAddr, owner)
		defer bc.Close()
		if async {
			bc.SetAsyncCommit(true)
		}

		deploy := makeBlockhashProbeDeployTx(owner)
		b1 := buildTxBlock(bc.CurrentBlock(), witnessAddr, deploy)
		if err := bc.InsertBlock(b1); err != nil {
			t.Fatalf("deploy block: %v", err)
		}
		bc.WaitForCommitSettled()
		txID := deploy.Hash()
		info := rawdb.ReadTransactionInfo(bc.chaindb, txID[:])
		if info == nil || len(info.ContractAddress) == 0 {
			t.Fatal("deploy txinfo missing contract address")
		}
		contractAddr := tcommon.BytesToAddress(info.ContractAddress)

		b2 := buildTxBlock(b1, witnessAddr)
		b3 := buildTxBlock(b2, witnessAddr, makeBlockhashProbeTriggerTx(owner, contractAddr))

		if async {
			// Deterministically lose the publish race: stall the commit worker
			// on b2 so b3's execution provably runs while b2's metadata batch
			// (its b-<num> row) has not reached Pebble.
			SetCommitFoldHookForTest(func(blockNum uint64) error {
				if blockNum == b2.Number() {
					time.Sleep(150 * time.Millisecond)
				}
				return nil
			})
			defer SetCommitFoldHookForTest(nil)
		}

		if err := bc.InsertBlocks([]*types.Block{b2, b3}); err != nil {
			t.Fatalf("InsertBlocks: %v", err)
		}
		bc.WaitForCommitSettled()
		bc.WaitForFlushSettled()

		statedb, err := bc.openState(bc.HeadStateRoot())
		if err != nil {
			t.Fatalf("openState: %v", err)
		}
		got := statedb.GetState(contractAddr, uint64ToDataWord(0))
		if want := b2.Hash(); got != want {
			t.Fatalf("blockhash(parent) stored by block %d: got %x, want %x", b3.Number(), got, want)
		}
	}

	// Control: the synchronous path persists the parent's metadata inline
	// before the next block executes, so it resolves today.
	t.Run("sync", func(t *testing.T) { run(t, false) })
	t.Run("async", func(t *testing.T) { run(t, true) })
}
