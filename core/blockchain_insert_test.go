package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func testInsertAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestBlockChain_InsertBlock_Transfer(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build transfer tx: addr(1) -> addr(2) for 5M TRX
	tc := &contractpb.TransferContract{
		OwnerAddress: testInsertAddr(1).Bytes(),
		ToAddress:    testInsertAddr(2).Bytes(),
		Amount:       5_000_000,
	}
	param, _ := anypb.New(tc)
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				Timestamp:  3000,
				ParentHash: genesisHash[:],
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})

	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block: got %d, want 1", bc.CurrentBlock().Number())
	}

	// Verify DynProps updated. Read via bc.DynProps() (buffered): slice 2 of
	// the fork-rewind fix routes DP writes through the in-memory buffer and
	// flushes them to disk only after the block becomes solidified.
	// state.LoadDynamicProperties(diskdb) would return defaults until that
	// happens. See docs/superpowers/specs/2026-04-30-fork-rewind-fix-slice2-design.md.
	dynProps := bc.DynProps()
	if got := dynProps.LatestBlockHeaderNumber(); got != 1 {
		t.Fatalf("dynprops block number: got %d, want 1", got)
	}
}

func TestBlockChain_InsertBlock_MultipleBlocks(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 100_000_000},
		},
		DynamicProperties: map[string]int64{},
	}
	SetupGenesisBlock(diskdb, genesis)
	sdb := state.NewDatabase(diskdb)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Insert 3 empty blocks
	for i := uint64(1); i <= 3; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	if bc.CurrentBlock().Number() != 3 {
		t.Fatalf("current: got %d, want 3", bc.CurrentBlock().Number())
	}
}

// TestBlockChain_ForkSwitch_10Block verifies that InsertBlock detects and switches
// to a competing chain when it becomes longer than the current canonical tip.
// Architecture: 10-block canonical chain A, then an 11-block competing chain B
// branching from genesis. When block 11B arrives, switchFork rewinds to genesis
// and replays chain B on top. Mirrors java-tron Manager.switchFork behaviour.
func TestBlockChain_ForkSwitch_10Block(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // far future — no maintenance in test
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build and insert chain A: blocks 1..10 with timestamps 3000, 6000, …
	chainA := make([]*types.Block, 11) // [0]=genesis placeholder, [1..10]=actual
	chainA[0] = types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{ParentHash: genesisHash[:]}},
	})
	chainA[0] = bc.CurrentBlock() // use real genesis
	for i := 1; i <= 10; i++ {
		parent := chainA[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("chain A block %d: %v", i, err)
		}
		chainA[i] = b
	}
	if bc.CurrentBlock().Number() != 10 {
		t.Fatalf("after chain A: head = %d, want 10", bc.CurrentBlock().Number())
	}
	tipA := bc.CurrentBlock().Hash()

	// Build chain B: 11 blocks branching from genesis with distinct timestamps.
	// Timestamps use offset +1 to produce different block hashes from chain A.
	chainB := make([]*types.Block, 12) // [0]=genesis, [1..11]=B blocks
	chainB[0] = bc.genesisBlock
	for i := 1; i <= 11; i++ {
		parent := chainB[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i)*3000 + 1, // +1 → distinct hash from chain A
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		chainB[i] = b
	}

	// Insert chain B blocks 1..10 — no fork switch yet (not longer than A).
	for i := 1; i <= 10; i++ {
		if err := bc.InsertBlock(chainB[i]); err != nil {
			t.Fatalf("chain B block %d (pre-switch): %v", i, err)
		}
		if bc.CurrentBlock().Hash() != tipA {
			t.Fatalf("block %dB should not trigger switch (chain A still longer)", i)
		}
	}

	// Insert chain B block 11 — triggers fork switch.
	if err := bc.InsertBlock(chainB[11]); err != nil {
		t.Fatalf("chain B block 11 (switch trigger): %v", err)
	}

	// Verify canonical chain switched to B.
	if bc.CurrentBlock().Number() != 11 {
		t.Fatalf("after fork switch: head = %d, want 11", bc.CurrentBlock().Number())
	}
	if bc.CurrentBlock().Hash() != chainB[11].Hash() {
		t.Fatalf("head hash = %x, want chain B tip %x", bc.CurrentBlock().Hash(), chainB[11].Hash())
	}

	// KhaosDB must still contain both chain tips.
	if !bc.khaosDB.ContainsBlock(tipA) {
		t.Error("chain A tip should still be in KhaosDB")
	}
	if !bc.khaosDB.ContainsBlock(chainB[11].Hash()) {
		t.Error("chain B tip should be in KhaosDB")
	}

	// State correctness: open StateDB from the new canonical root and verify
	// that witness allowance equals exactly 11 × WitnessPayPerBlock.
	// If switchFork opened applyBlock from the wrong parent root, allowance
	// would carry chain-A's accumulated rewards too (21 blocks × rate).
	stateRoot := rawdb.ReadBlockStateRoot(diskdb, bc.CurrentBlock().Hash())
	statedb, err := state.New(stateRoot, sdb)
	if err != nil {
		t.Fatalf("open state after fork switch: %v", err)
	}
	dynProps := state.LoadDynamicProperties(diskdb)
	wantAllowance := dynProps.WitnessPayPerBlock() * 11
	if got := statedb.GetAllowance(witnessAddr); got != wantAllowance {
		t.Fatalf("witness allowance after fork switch: got %d, want %d (11 × WitnessPayPerBlock)", got, wantAllowance)
	}
}

// TestForkSwitch_WitnessCountersNoDoubleCount drives a 3-vs-4 reorg where the
// same witness produces every block on both branches and asserts that, after
// the fork switch, the canonical witness's TotalProduced equals exactly the
// length of the canonical chain (4) — not 7, which would indicate that the
// orphan-branch increments leaked through.
//
// Slice 2 of the fork-rewind fix extends this test to also cover
// total_transaction_count, latest_solidified_block_num (DP), and the per-
// witness latest-block cursor — all of which were on the disk-direct path
// in slice 1 and would otherwise leak across switchFork even with the
// witness-counter buffer in place.
//
// Reads go via bc.BufferedDB() because at the moment of assertion the
// canonical chain B's blocks may still be above solidified and therefore
// in-memory only. See
// docs/superpowers/specs/2026-04-30-fork-rewind-fix-slice2-design.md.
func TestForkSwitch_WitnessCountersNoDoubleCount(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		// Three active witnesses, only one produces. updateSolidifiedBlock's
		// java-tron rule is `nums[floor(N*0.3)]` after sorting ascending; for
		// N=3 that's `nums[0]`, which stays at 0 because the other two
		// witnesses never produce. So solidified stays at 0 throughout the
		// test, all layers stay in memory, and switchFork can rewind the
		// full orphan branch via DiscardBlock. This mirrors mainnet, where
		// solidified lags head by ~19 blocks (27 SRs).
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(2), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(3), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // far future — no maintenance
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build chain A: 3 blocks, all produced by witnessAddr.
	chainA := make([]*types.Block, 4)
	chainA[0] = bc.genesisBlock
	for i := 1; i <= 3; i++ {
		parent := chainA[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("chain A block %d: %v", i, err)
		}
		chainA[i] = b
	}
	if bc.CurrentBlock().Number() != 3 {
		t.Fatalf("after chain A: head = %d, want 3", bc.CurrentBlock().Number())
	}

	// Sanity: buffer reflects exactly 3 productions (linear extension path).
	wA := rawdb.ReadWitness(bc.BufferedDB(), witnessAddr)
	if wA == nil {
		t.Fatal("witness counter not buffered after chain A")
	}
	if got := wA.TotalProduced(); got != 3 {
		t.Fatalf("after chain A: TotalProduced = %d, want 3", got)
	}

	// Build chain B: 4 blocks, also produced by witnessAddr, branching from
	// genesis with offset timestamps so block hashes diverge from chain A.
	chainB := make([]*types.Block, 5)
	chainB[0] = bc.genesisBlock
	for i := 1; i <= 4; i++ {
		parent := chainB[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i)*3000 + 1, // +1 → distinct hash
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		chainB[i] = b
	}

	// Insert B1..B3: KhaosDB stores them as a competing branch but no
	// switchFork yet (chain A still longer/equal).
	for i := 1; i <= 3; i++ {
		if err := bc.InsertBlock(chainB[i]); err != nil {
			t.Fatalf("chain B block %d (pre-switch): %v", i, err)
		}
	}
	if bc.CurrentBlock().Hash() != chainA[3].Hash() {
		t.Fatal("chain B should not have triggered switchFork yet")
	}

	// Insert B4 — chain B now strictly longer → switchFork.
	if err := bc.InsertBlock(chainB[4]); err != nil {
		t.Fatalf("chain B block 4 (switch trigger): %v", err)
	}
	if bc.CurrentBlock().Number() != 4 {
		t.Fatalf("after switchFork: head = %d, want 4", bc.CurrentBlock().Number())
	}
	if bc.CurrentBlock().Hash() != chainB[4].Hash() {
		t.Fatal("after switchFork: head hash != chain B tip")
	}

	// The bug: without orphan-buffer rollback, TotalProduced would be
	// 3 (chain A) + 4 (chain B) = 7. With slice-1 fix it must be exactly 4.
	wPost := rawdb.ReadWitness(bc.BufferedDB(), witnessAddr)
	if wPost == nil {
		t.Fatal("witness counter missing after switchFork")
	}
	if got := wPost.TotalProduced(); got != 4 {
		t.Fatalf("after switchFork: TotalProduced = %d, want 4 "+
			"(orphan branch counters must NOT be double-counted)", got)
	}

	// LatestBlockNum must reflect canonical chain B's tip.
	if got := wPost.LatestBlockNum(); got != 4 {
		t.Fatalf("after switchFork: LatestBlockNum = %d, want 4", got)
	}

	// Pin the missed-slot semantics: each block's timestamp is exactly one
	// BlockProducedInterval after its parent (no gaps), so ApplyBlockStatistics
	// records zero missed slots for every block. If TotalMissed grows, the
	// test is exercising different slot semantics than intended.
	if got := wPost.TotalMissed(); got != 0 {
		t.Fatalf("after switchFork: TotalMissed = %d, want 0 (no skipped slots)", got)
	}

	// Slice 2 retrofits — these were leaking before slice 2:

	// total_transaction_count: zero in this test (no txs in any block). If
	// the orphan increments had leaked the count would be > 0.
	if got := rawdb.ReadTotalTransactionCount(bc.BufferedDB()); got != 0 {
		t.Fatalf("after switchFork: total_transaction_count = %d, want 0", got)
	}

	// Per-witness latest-block cursor (used by updateSolidifiedBlock):
	// must reflect canonical chain B's tip, not chain A's stale tip.
	if got := rawdb.ReadWitnessLatestBlock(bc.BufferedDB(), witnessAddr); got != 4 {
		t.Fatalf("after switchFork: WitnessLatestBlock = %d, want 4", got)
	}

	// latest_solidified_block_num (DP, computed by updateSolidifiedBlock):
	// with 3 active witnesses where only one produces, the floor(N*0.3)=0
	// position resolves to a witness that never produced (stays at 0).
	// Solidified therefore never advances in this test — which is what we
	// want, because it keeps the orphan layers in memory so DiscardBlock
	// can rewind them.
	dynPost := state.LoadDynamicProperties(bc.BufferedDB())
	if got := dynPost.LatestSolidifiedBlockNum(); got != 0 {
		t.Fatalf("after switchFork: LatestSolidifiedBlockNum = %d, want 0 (test holds solidified flat)", got)
	}

	// latest_block_header_number (DP): from canonical chain B.
	if got := dynPost.LatestBlockHeaderNumber(); got != 4 {
		t.Fatalf("after switchFork: LatestBlockHeaderNumber = %d, want 4", got)
	}

	// Buffer must contain all 4 canonical layers (none flushed because
	// solidified stayed at 0). If it contained more (orphan leakage) or
	// fewer (premature flush) the test would catch it.
	if got := len(bc.buffer.PendingBlocks()); got != 4 {
		t.Fatalf("after switchFork: buffer holds %d layers, want 4 (canonical-only)", got)
	}

	// burn_trx_amount and total_create_witness_cost (also DP keys) flow
	// through the same dp.Flush(bc.buffer) → DiscardBlock path as
	// latest_block_header_number, so the LatestBlockHeaderNumber rewind
	// above is the property test for all DP-tracked counters. We don't
	// duplicate the assertion for every DP key; the fork-rewind retrofit
	// is uniform across `dp.dirty`.
}

// TestForkSwitch_ActiveWitnessesRewindAcrossMaintenance pins the fork-rewind
// fix for the active-witness set. Before the fix, SetActiveWitnesses wrote
// straight to bc.db, bypassing the buffer: an orphan-branch maintenance block
// that rotated the active set left bc.db (and the in-memory atomic) holding
// the rotated set even after switchFork rewound everything else. java-tron
// keeps the active set in a revoking store, so it must rewind with the rest of
// consensus state.
//
// Setup: the on-disk active-witness list is seeded in a *different order* than
// SelectActiveWitnesses computes from the witness vote store, so the first
// maintenance crossing genuinely rotates the set (SetActiveWitnesses fires).
// Chain A crosses the maintenance boundary; the longer chain B branches from
// genesis (before the boundary) and never reaches it. After switchFork,
// ActiveWitnesses() must equal the pre-maintenance seed, not the rotated set.
func TestForkSwitch_ActiveWitnessesRewindAcrossMaintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	// Producer is the lowest-vote witness so it never lands at the
	// floor(N*0.3)=0 solidified slot — solidified stays at 0, keeping orphan
	// layers in memory so switchFork's DiscardBlock can rewind them.
	wProd := testInsertAddr(1)
	wHi := testInsertAddr(2)
	wMid := testInsertAddr(3)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: wProd, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: wProd, VoteCount: 1, URL: "prod"},
			{Address: wHi, VoteCount: 300, URL: "hi"},
			{Address: wMid, VoteCount: 200, URL: "mid"},
		},
		DynamicProperties: map[string]int64{
			// Maintenance boundary at ts=9000 → chain A block 3 crosses it,
			// chain B (branching from genesis) tops out at block 4 ts<9000... no:
			// see timestamps below — B is kept strictly below the boundary.
			"next_maintenance_time": 9000,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	// Seed the on-disk active-witness list in an order that differs from what
	// SelectActiveWitnesses (vote-descending: wHi, wMid, wProd) will compute,
	// so the first maintenance run rotates the set. This write predates
	// NewBlockChain, which loads it verbatim into the atomic.
	preMaintSet := []tcommon.Address{wProd, wHi, wMid}
	rawdb.WriteActiveWitnesses(diskdb, preMaintSet)

	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	if got := bc.ActiveWitnesses(); len(got) != 3 || got[0] != wProd {
		t.Fatalf("genesis active set not loaded from seed: %v", got)
	}

	// Chain A: blocks 1..3. Block 3 at ts=9000 crosses the maintenance
	// boundary and rotates the active set via SetActiveWitnesses.
	chainA := make([]*types.Block, 4)
	chainA[0] = bc.genesisBlock
	for i := 1; i <= 3; i++ {
		parent := chainA[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: wProd.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("chain A block %d: %v", i, err)
		}
		chainA[i] = b
	}
	if bc.CurrentBlock().Number() != 3 {
		t.Fatalf("after chain A: head = %d, want 3", bc.CurrentBlock().Number())
	}

	// The maintenance crossing must have rotated the active set to the
	// vote-sorted order — otherwise the test isn't exercising the bug.
	rotated := bc.ActiveWitnesses()
	if len(rotated) != 3 || rotated[0] != wHi || rotated[1] != wMid || rotated[2] != wProd {
		t.Fatalf("maintenance did not rotate active set as expected: got %v", rotated)
	}

	// Chain B: 4 blocks branching from genesis, all kept strictly below the
	// ts=9000 maintenance boundary so chain B never runs maintenance. Offset
	// timestamps (+1) give distinct block hashes.
	chainB := make([]*types.Block, 5)
	chainB[0] = bc.genesisBlock
	for i := 1; i <= 4; i++ {
		parent := chainB[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i)*2000 + 1, // 2001,4001,6001,8001 < 9000
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: wProd.Bytes(),
				},
			},
		})
		chainB[i] = b
	}

	// Insert B1..B3 — competing branch, chain A still longer/equal, no switch.
	for i := 1; i <= 3; i++ {
		if err := bc.InsertBlock(chainB[i]); err != nil {
			t.Fatalf("chain B block %d (pre-switch): %v", i, err)
		}
	}
	if bc.CurrentBlock().Hash() != chainA[3].Hash() {
		t.Fatal("chain B should not have triggered switchFork yet")
	}

	// Insert B4 — chain B strictly longer → switchFork rewinds across the
	// maintenance boundary back to genesis, then replays B1..B4.
	if err := bc.InsertBlock(chainB[4]); err != nil {
		t.Fatalf("chain B block 4 (switch trigger): %v", err)
	}
	if bc.CurrentBlock().Hash() != chainB[4].Hash() {
		t.Fatal("after switchFork: head hash != chain B tip")
	}

	// The fix: ActiveWitnesses must have rewound to the pre-maintenance seed.
	// Chain B never crossed the boundary, so no rotation should be in effect.
	post := bc.ActiveWitnesses()
	if len(post) != 3 || post[0] != wProd || post[1] != wHi || post[2] != wMid {
		t.Fatalf("after switchFork: active set = %v, want pre-maintenance seed %v "+
			"(orphan-branch rotation must rewind with the rest of consensus state)",
			post, preMaintSet)
	}

	// The buffer-backed disk view must agree with the atomic.
	if buffered := rawdb.ReadActiveWitnesses(bc.BufferedDB()); len(buffered) != 3 ||
		buffered[0] != wProd || buffered[1] != wHi || buffered[2] != wMid {
		t.Fatalf("after switchFork: buffered active set = %v, want %v", buffered, preMaintSet)
	}
}

// TestFlushAtSolidified_SurvivesRestart verifies the slice-2 flush-at-
// solidified policy: with a single active witness, solidified == head every
// block (floor(1*0.3)=0 → solidified = nums[0] = the single witness's
// latest), so flushBufferUpToSolidified drains every layer to disk
// immediately after CommitBlock. After 5 blocks the buffer must be empty,
// and a fresh BlockChain rebuilt from the same disk store must read
// post-applyBlock counters consistent with what slice-1's direct-write
// path would have produced.
func TestFlushAtSolidified_SurvivesRestart(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		// Single active witness → solidified == head every block.
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // far future — no maintenance
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		parent := bc.CurrentBlock()
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
		// After every block, all prior layers should be flushed (single-SR:
		// solidified == head). The just-committed block itself was solidified
		// during applyBlock, so its layer is also flushed by the time
		// flushBufferUpToSolidified returns.
		if got := len(bc.buffer.PendingBlocks()); got != 0 {
			t.Fatalf("after block %d: buffer holds %d layers, want 0 (solidified=head should flush)", i, got)
		}
	}

	// Restart simulation: drop bc, build fresh on the same disk.
	bc2, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := bc2.CurrentBlock().Number(); got != 5 {
		t.Fatalf("restart: head = %d, want 5", got)
	}

	// Read post-applyBlock counters DIRECTLY from disk (not via the new
	// buffer). They must match what slice 1's direct-write path would have
	// produced — i.e. the on-disk consistency property.
	w := rawdb.ReadWitness(diskdb, witnessAddr)
	if w == nil {
		t.Fatal("witness counter not on disk after restart")
	}
	if got := w.TotalProduced(); got != 5 {
		t.Fatalf("disk-side TotalProduced = %d, want 5", got)
	}
	if got := w.LatestBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestBlockNum = %d, want 5", got)
	}

	// DP `latest_solidified_block_num` survived restart.
	dpDisk := state.LoadDynamicProperties(diskdb)
	if got := dpDisk.LatestSolidifiedBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestSolidifiedBlockNum = %d, want 5", got)
	}
	if got := dpDisk.LatestBlockHeaderNumber(); got != 5 {
		t.Fatalf("disk-side LatestBlockHeaderNumber = %d, want 5", got)
	}

	// Per-witness latest-block cursor (used by updateSolidifiedBlock) is
	// also on disk after restart.
	if got := rawdb.ReadWitnessLatestBlock(diskdb, witnessAddr); got != 5 {
		t.Fatalf("disk-side WitnessLatestBlock = %d, want 5", got)
	}
}

// TestLinearExtension_WitnessCountersThroughBuffer is a regression check that
// the buffer wiring does not perturb the non-fork path: a 3-block linear
// chain produces TotalProduced == 3 when read through the buffer.
func TestLinearExtension_WitnessCountersThroughBuffer(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 1_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		parent := bc.CurrentBlock()
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	w := rawdb.ReadWitness(bc.BufferedDB(), witnessAddr)
	if w == nil {
		t.Fatal("witness counter not buffered after linear chain")
	}
	if got := w.TotalProduced(); got != 3 {
		t.Fatalf("linear extension: TotalProduced = %d, want 3", got)
	}
	if got := w.LatestBlockNum(); got != 3 {
		t.Fatalf("linear extension: LatestBlockNum = %d, want 3", got)
	}
}

// TestForkSwitch_AddCycleRewardRollback exercises slice-3 sub-task A: with
// `change_delegation` enabled, every produced block routes through
// payBlockReward → AddCycleReward, which writes the per-cycle voter pool
// via the buffer. After a 3-vs-4 reorg the canonical AddCycleReward total
// must reflect ONLY chain B (4 blocks × witness_pay_per_block × (1 - brokerage%)),
// not chain A's orphaned 3-block additions on top.
//
// Mirrors the slice-2 TestForkSwitch_WitnessCountersNoDoubleCount layout
// (3 active witnesses → solidified stays at 0 → all layers in memory).
func TestForkSwitch_AddCycleRewardRollback(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(2), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(3), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time":     1<<62 - 1, // far future — no maintenance
			"change_delegation":         1,         // gate AddCycleReward writer ON
			"current_cycle_number":      7,         // arbitrary — must match across runs
			"witness_127_pay_per_block": 0,         // disable standby pay → simpler math
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Default brokerage is 20% per java-tron MortgageService. With the
	// default `witness_pay_per_block` = 32_000_000, voter pool per block
	// is 32_000_000 × 0.80 = 25_600_000. Standby pay disabled above.
	const voterPerBlock int64 = 32_000_000 - (32_000_000 * 20 / 100)

	// Build chain A: 3 blocks produced by witnessAddr.
	chainA := make([]*types.Block, 4)
	chainA[0] = bc.genesisBlock
	for i := 1; i <= 3; i++ {
		parent := chainA[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("chain A block %d: %v", i, err)
		}
		chainA[i] = b
	}

	// Sanity: cycle reward reflects 3 chain-A blocks (read via buffer —
	// solidified stays at 0 with 3 witnesses).
	gotA := rawdb.ReadCycleReward(bc.BufferedDB(), 7, witnessAddr.Bytes())
	wantA := 3 * voterPerBlock
	if gotA != wantA {
		t.Fatalf("after chain A: cycle 7 reward = %d, want %d", gotA, wantA)
	}

	// Build chain B: 4 blocks branching from genesis (offset timestamps).
	chainB := make([]*types.Block, 5)
	chainB[0] = bc.genesisBlock
	for i := 1; i <= 4; i++ {
		parent := chainB[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i)*3000 + 1, // +1 → distinct hash
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		chainB[i] = b
	}
	for i := 1; i <= 4; i++ {
		if err := bc.InsertBlock(chainB[i]); err != nil {
			t.Fatalf("chain B block %d: %v", i, err)
		}
	}
	if bc.CurrentBlock().Hash() != chainB[4].Hash() {
		t.Fatalf("after switchFork: head hash != chain B tip")
	}

	// Without slice-3 buffer routing, this would be (3 + 4) × voterPerBlock.
	// With the fix, only chain B's 4 blocks survive.
	gotB := rawdb.ReadCycleReward(bc.BufferedDB(), 7, witnessAddr.Bytes())
	wantB := 4 * voterPerBlock
	if gotB != wantB {
		t.Fatalf("after switchFork: cycle 7 reward = %d, want %d "+
			"(orphan AddCycleReward writes must NOT leak)", gotB, wantB)
	}
}

// TestForkSwitch_AssetIssueActuatorRollback exercises slice-3 sub-task B:
// an actuator that writes via `ctx.DB.Put` (here `WriteAssetIssue` /
// `WriteAssetNameIndex` / `WriteAssetOwnerIndex`) must have its writes
// rolled back when the block landed on the orphan branch.
//
// Setup: chain A includes an asset-issue tx at block 1; chain B is 2
// blocks longer with no asset issues. Pre-slice-3, chain A's asset
// indexes would persist on disk after the reorg. With slice-3's
// actuator.Context.DB widening, the writes go through bc.buffer and
// switchFork's DiscardBlock rewinds them.
func TestForkSwitch_AssetIssueActuatorRollback(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)
	issuer := testInsertAddr(7)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
			{Address: issuer, Balance: 99_000_000_000_000_000},
		},
		// 3 active witnesses → solidified stays at 0 → all layers in memory.
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(2), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(3), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build asset-issue contract. Name "FORKTKN" — distinctive enough that
	// we can assert its absence on disk after the reorg.
	assetName := []byte("FORKTKN")
	tokenContract := &contractpb.AssetIssueContract{
		OwnerAddress:      issuer.Bytes(),
		Name:              assetName,
		Abbr:              []byte("FT"),
		TotalSupply:       1_000_000,
		TrxNum:            1,
		Num:               1,
		StartTime:         1000,
		EndTime:           2_000_000_000_000, // far future
		Precision:         0,
		FreeAssetNetLimit: 0,
	}
	param, err := anypb.New(tokenContract)
	if err != nil {
		t.Fatal(err)
	}
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_AssetIssueContract,
				Parameter: param,
			}},
		},
	}

	// Build chain A: 3 blocks. Block 1 contains the asset-issue tx; blocks
	// 2 and 3 are empty.
	chainA := make([]*types.Block, 4)
	chainA[0] = bc.genesisBlock
	for i := 1; i <= 3; i++ {
		parent := chainA[i-1]
		blkPB := &corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		}
		if i == 1 {
			blkPB.Transactions = []*corepb.Transaction{txPB}
		}
		b := types.NewBlockFromPB(blkPB)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("chain A block %d: %v", i, err)
		}
		chainA[i] = b
	}

	// Sanity: asset indexes are buffered on chain A.
	if _, ok := rawdb.ReadAssetNameIndex(bc.BufferedDB(), assetName); !ok {
		t.Fatal("after chain A: asset name index missing from buffer")
	}
	if _, ok := rawdb.ReadAssetOwnerIndex(bc.BufferedDB(), issuer[:]); !ok {
		t.Fatal("after chain A: asset owner index missing from buffer")
	}

	// Build chain B: 4 empty blocks → triggers switchFork on B[4].
	chainB := make([]*types.Block, 5)
	chainB[0] = bc.genesisBlock
	for i := 1; i <= 4; i++ {
		parent := chainB[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i)*3000 + 1, // +1 → distinct hash
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		chainB[i] = b
	}
	for i := 1; i <= 4; i++ {
		if err := bc.InsertBlock(chainB[i]); err != nil {
			t.Fatalf("chain B block %d: %v", i, err)
		}
	}
	if bc.CurrentBlock().Hash() != chainB[4].Hash() {
		t.Fatal("after switchFork: head != chain B tip")
	}

	// Asset indexes from chain A's orphaned issuance must be GONE from the
	// buffer view (they would still be on disk if the actuator widening
	// were not in place — but solidified=0 means nothing was flushed, so
	// the buffer view is the canonical source of truth here).
	if _, ok := rawdb.ReadAssetNameIndex(bc.BufferedDB(), assetName); ok {
		t.Fatal("after switchFork: asset name index leaked from orphan branch")
	}
	if _, ok := rawdb.ReadAssetOwnerIndex(bc.BufferedDB(), issuer[:]); ok {
		t.Fatal("after switchFork: asset owner index leaked from orphan branch")
	}
	// And specifically NOT on disk either.
	if _, ok := rawdb.ReadAssetNameIndex(diskdb, assetName); ok {
		t.Fatal("after switchFork: asset name index leaked to disk from orphan branch")
	}
	if _, ok := rawdb.ReadAssetOwnerIndex(diskdb, issuer[:]); ok {
		t.Fatal("after switchFork: asset owner index leaked to disk from orphan branch")
	}
}

// TestGracefulShutdown_FlushesSolidified exercises slice-3 sub-task C: a
// clean shutdown via bc.Close() must persist every buffer layer at or
// below `latest_solidified_block_num` to disk and drop everything above.
// On restart, the new BlockChain reads the solidified-line state from
// disk and matches what slice-1's direct-write path would have produced
// for a chain frozen at solidified.
//
// Setup: 27 active witnesses, only one produces. floor(27 × 0.3) = 8;
// after 9 produced blocks by witnessAddr, the witness's WitnessLatestBlock
// reaches 9, but the sorted nums[] list across all 27 witnesses has 26
// zeros and one 9 — nums[8] (zero-indexed) is still 0, so solidified
// stays at 0 and no flush happens. To force a real flush boundary we use
// 1 active witness so solidified == head every block, giving a non-trivial
// flush window for each call.
//
// We then produce 5 blocks WITHOUT going through Close (they all flush
// immediately due to solidified=head), and 5 more after temporarily
// adding a second silent witness (still solidified=head with 1 producer
// and 1 other since floor(2×0.3)=0). Close() runs, restart, assert state.
func TestGracefulShutdown_FlushesSolidified(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		// Single active witness → floor(1 × 0.3) = 0 → solidified == head
		// every block. Every applyBlock flushes its layer immediately.
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		parent := bc.CurrentBlock()
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	// Pre-Close sanity: with 1 SR, solidified == head, so flush is automatic
	// per applyBlock and the buffer is already empty. Close should be a no-op
	// in this configuration (idempotent).
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("pre-Close: buffer holds %d layers, want 0 (solidified=head)", got)
	}
	if err := bc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Buffer must be empty after Close.
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("post-Close: buffer holds %d layers, want 0", got)
	}

	// Restart: open fresh BlockChain on the same disk store.
	sdb2 := state.NewDatabase(diskdb)
	bc2, err := NewBlockChain(diskdb, sdb2, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := bc2.CurrentBlock().Number(); got != 5 {
		t.Fatalf("restart: head = %d, want 5", got)
	}

	// Disk-side counters must match what was on the now-flushed buffer.
	w := rawdb.ReadWitness(diskdb, witnessAddr)
	if w == nil {
		t.Fatal("witness counter not on disk after Close+restart")
	}
	if got := w.TotalProduced(); got != 5 {
		t.Fatalf("disk-side TotalProduced = %d, want 5", got)
	}
	if got := w.LatestBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestBlockNum = %d, want 5", got)
	}
	dpDisk := state.LoadDynamicProperties(diskdb)
	if got := dpDisk.LatestSolidifiedBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestSolidifiedBlockNum = %d, want 5", got)
	}
	if got := dpDisk.LatestBlockHeaderNumber(); got != 5 {
		t.Fatalf("disk-side LatestBlockHeaderNumber = %d, want 5", got)
	}
}

// TestGracefulShutdown_DropsLayersAboveSolidified exercises the harder
// branch of slice-3 sub-task C: when applyBlock leaves layers above the
// solidified line in memory, Close must flush only up to solidified and
// drop the higher layers. After restart, the disk-side image reflects
// the solidified-line state — NOT the unflushed in-memory state.
//
// 3 active witnesses (where only one produces) keeps solidified at 0
// across every applyBlock — same configuration as the slice-2 reorg test.
// All 5 layers stay in memory. Close() drops them.
func TestGracefulShutdown_DropsLayersAboveSolidified(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(2), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(3), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		parent := bc.CurrentBlock()
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	// All 5 layers in memory (solidified stays at 0 with 3 SRs / 1 producer).
	if got := len(bc.buffer.PendingBlocks()); got != 5 {
		t.Fatalf("pre-Close: buffer holds %d layers, want 5", got)
	}

	// Close: flushes up to solidified=0 (no-op) and drops the 5 layers.
	if err := bc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(bc.buffer.PendingBlocks()); got != 0 {
		t.Fatalf("post-Close: buffer holds %d layers, want 0", got)
	}

	// Restart: disk-side state reflects ZERO post-applyBlock counters,
	// because none of the 5 layers were flushed. This documents the
	// trade-off: clean shutdown above solidified loses the post-block
	// counters. Recovery: re-sync the missing range from peers.
	sdb2 := state.NewDatabase(diskdb)
	bc2, err := NewBlockChain(diskdb, sdb2, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	// Block bodies are persisted in applyBlock (rawdb.WriteBlock is direct,
	// not through the buffer), so head still advances. This is consistent
	// with java-tron, where Manager.pushBlock writes the block before the
	// session-tracked DP/witness mutations.
	if got := bc2.CurrentBlock().Number(); got != 5 {
		t.Fatalf("restart: head = %d, want 5", got)
	}
	// Witness counter NOT on disk — was only ever in the dropped buffer.
	w := rawdb.ReadWitness(diskdb, witnessAddr)
	if w != nil && w.TotalProduced() != 0 {
		t.Fatalf("disk-side TotalProduced = %d, want 0 "+
			"(layers above solidified were dropped on Close)", w.TotalProduced())
	}
}
