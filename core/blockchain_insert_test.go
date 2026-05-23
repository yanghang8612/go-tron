package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
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
			Expiration: 60_000,
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

func TestBlockChain_InsertBlockUpdatesForkStats(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	w1 := testInsertAddr(1)
	w2 := testInsertAddr(2)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: w1, Balance: 100_000_000},
			{Address: w2, Balance: 100_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: w1, VoteCount: 1000, URL: "http://w1"},
			{Address: w2, VoteCount: 1000, URL: "http://w2"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	for i, witness := range []tcommon.Address{w1, w2} {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i + 1),
					Timestamp:      1_600_000_000_000 + int64(i+1)*3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witness.Bytes(),
					Version:        params.BlockVersion,
				},
			},
		})
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("block %d: %v", i+1, err)
		}
	}

	stats := rawdb.ReadForkStats(bc.buffer, 28)
	if len(stats) != 2 {
		t.Fatalf("v28 stats len: got %d, want 2", len(stats))
	}
	for i, got := range stats {
		if got != forks.VoteUpgrade {
			t.Fatalf("v28 stats slot %d: got %d, want upgrade", i, got)
		}
	}
	if !bc.ForkController().Pass(28, bc.CurrentBlock().Timestamp(), bc.DynProps().MaintenanceTimeInterval()) {
		t.Fatal("v28 should pass after both active witnesses produced v35 blocks")
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
	//
	// switchFork drains the in-flight async-flush queue at its prologue, but
	// the re-apply loop that follows enqueues fresh cutoffs for each block
	// in the new branch. Wait for those to settle before reading diskdb so
	// the test observes the post-reorg state, not a partial flush.
	bc.WaitForFlushSettled()
	stateRoot := rawdb.ReadBlockStateRoot(bc.chaindb, bc.CurrentBlock().Hash())
	statedb, err := state.New(stateRoot, sdb)
	if err != nil {
		t.Fatalf("open state after fork switch: %v", err)
	}
	dynProps := state.LoadDynamicProperties(diskdb, statedb)
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

	// Sanity: head state reflects exactly 3 productions (linear extension path).
	wA := readWitnessAtHead(t, bc, witnessAddr)
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
	wPost := readWitnessAtHead(t, bc, witnessAddr)
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
	if got := readWitnessLatestBlockAtHead(t, bc, witnessAddr); got != 4 {
		t.Fatalf("after switchFork: WitnessLatestBlock = %d, want 4", got)
	}

	// latest_solidified_block_num (DP, computed by updateSolidifiedBlock):
	// with 3 active witnesses where only one produces, the floor(N*0.3)=0
	// position resolves to a witness that never produced (stays at 0).
	// Solidified therefore never advances in this test — which is what we
	// want, because it keeps the orphan layers in memory so DiscardBlock
	// can rewind them.
	dynPost := state.LoadDynamicProperties(bc.BufferedDB(), nil) // derived-only read
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
	// Genesis roots the active set as SelectActiveWitnesses(votes) =
	// [wHi, wMid, wProd]; NewBlockChain loads it from the system-KV at the head
	// root. The maintenance boundary recomputes from the post-vote tallies below.
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	if got := bc.ActiveWitnesses(); len(got) != 3 || got[0] != wHi || got[1] != wMid || got[2] != wProd {
		t.Fatalf("genesis-rooted active set not loaded: got %v, want [wHi, wMid, wProd]", got)
	}
	// wProd casts 150 new votes for wMid, lifting it 200→350 above wHi (300).
	// At the maintenance boundary this flips the vote-ranked order from the
	// genesis-rooted [wHi, wMid, wProd] to [wMid, wHi, wProd] — a genuine
	// rotation that switchFork must rewind. (The active list is rooted into the
	// state, not the flat store, so the "old" set can't be pre-seeded in a
	// drifted order; a real vote delta drives the rotation instead.) The vote is
	// now also rooted (WitnessVoteState KV), so it must be seeded into the
	// genesis state root — chain B branches from genesis without crossing the
	// boundary, so the seeded-but-undrained vote rewinds with it.
	seedPendingVotesAtGenesis(t, bc, map[tcommon.Address]*corepb.Votes{
		wProd: {
			Address:  wProd.Bytes(),
			NewVotes: []*corepb.Vote{{VoteAddress: wMid.Bytes(), VoteCount: 150}},
		},
	})

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

	// The maintenance crossing must have rotated the active set to the new
	// vote-sorted order (wMid overtook wHi) — otherwise the test isn't
	// exercising the rewind.
	rotated := bc.ActiveWitnesses()
	if len(rotated) != 3 || rotated[0] != wMid || rotated[1] != wHi || rotated[2] != wProd {
		t.Fatalf("maintenance did not rotate active set as expected: got %v, want [wMid, wHi, wProd]", rotated)
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

	// The fix: ActiveWitnesses must have rewound to the genesis-rooted set.
	// Chain B branches from genesis and never crossed the boundary, so the
	// rotation must be undone — the active list rewinds with the rest of
	// consensus state via reloadActiveWitnesses(lcaRoot).
	post := bc.ActiveWitnesses()
	if len(post) != 3 || post[0] != wHi || post[1] != wMid || post[2] != wProd {
		t.Fatalf("after switchFork: active set = %v, want genesis-rooted [wHi, wMid, wProd] "+
			"(orphan-branch rotation must rewind with the rest of consensus state)", post)
	}
}

// TestForkSwitch_WitnessScheduleRewindDualMechanism is the Phase 3c gate for the
// two-mechanism rewind. Across a maintenance boundary the active witness list
// changes (rooted — rewinds via reloadActiveWitnesses(lcaRoot)) AND witness
// is_jobs flags flip (capsule — rewinds via the bc.buffer DiscardBlock). After a
// switchFork that rewinds across the boundary, BOTH must reflect the
// pre-maintenance (genesis) state, proving the root-based and buffer-based
// rewinds stay consistent. Pre-3c suites exercised only one mechanism at a time.
func TestForkSwitch_WitnessScheduleRewindDualMechanism(t *testing.T) {
	const interval = int64(21_600_000) // 6h
	const numWitnesses = 28            // 27 active + 1 standby (#27)
	witnessAddr := func(i int) tcommon.Address { return testCoreAddr(byte(40 + i)) }
	// Strictly-decreasing votes → genesis active = vote-ranked [0..26]; #27 standby.
	initialVote := func(i int) int64 { return 100_000 - int64(i)*100 }

	diskdb := ethrawdb.NewMemoryDatabase()
	genesisWitnesses := make([]params.GenesisWitness, numWitnesses)
	accounts := []params.GenesisAccount{{Address: testCoreAddr(1), Balance: 100_000_000}}
	for i := 0; i < numWitnesses; i++ {
		genesisWitnesses[i] = params.GenesisWitness{Address: witnessAddr(i), VoteCount: initialVote(i), URL: "http://w"}
		accounts = append(accounts, params.GenesisAccount{Address: witnessAddr(i), Balance: 1_000_000})
	}
	genesis := &params.Genesis{
		Config:            params.MainnetChainConfig,
		Timestamp:         0,
		Accounts:          accounts,
		Witnesses:         genesisWitnesses,
		DynamicProperties: map[string]int64{"next_maintenance_time": interval},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Genesis flips is_jobs=true on every witness; clear it on standby #27 so the
	// boundary's "incoming -> is_jobs=true" flip is a discriminating signal.
	w27 := readWitnessAtHead(t, bc, witnessAddr(27)).Copy()
	w27.SetIsJobs(false)
	seedGenesisWitnessCapsule(t, bc, w27)

	// Pending vote: +150 for #27 (97300→97450) lifts it above #26 (97400) but
	// below #25 (97500). At the boundary the active set swaps #27 in / #26 out —
	// a membership change that flips is_jobs for both. The vote is rooted
	// (WitnessVoteState KV), so it must be seeded into the genesis state root;
	// chain B branches from genesis without crossing the boundary, so the
	// seeded-but-undrained vote rewinds with it.
	seedPendingVotesAtGenesis(t, bc, map[tcommon.Address]*corepb.Votes{
		testCoreAddr(1): {
			Address:  testCoreAddr(1).Bytes(),
			NewVotes: []*corepb.Vote{{VoteAddress: witnessAddr(27).Bytes(), VoteCount: 150}},
		},
	})

	isJobs := func(i int) bool {
		return readWitnessAtHead(t, bc, witnessAddr(i)).IsJobs()
	}
	active := func() map[tcommon.Address]bool {
		m := map[tcommon.Address]bool{}
		for _, a := range bc.ActiveWitnesses() {
			m[a] = true
		}
		return m
	}

	// Pre-maintenance (genesis-rooted): #26 active+is_jobs, #27 standby+!is_jobs.
	if a := active(); !a[witnessAddr(26)] || a[witnessAddr(27)] {
		t.Fatalf("pre-maint active: in26=%v in27=%v, want true/false", a[witnessAddr(26)], a[witnessAddr(27)])
	}
	if !isJobs(26) || isJobs(27) {
		t.Fatalf("pre-maint is_jobs: #26=%v #27=%v, want true/false", isJobs(26), isJobs(27))
	}

	mkBlock := func(parent *types.Block, num, ts int64) *types.Block {
		return types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         num,
					Timestamp:      ts,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr(0).Bytes(),
				},
			},
		})
	}

	// Chain A: block #1 pre-boundary, block #2 at ts=interval crosses it (java
	// skips doMaintenance on block #1, so the rotation lands on block #2).
	a1 := mkBlock(bc.genesisBlock, 1, interval/2)
	if err := bc.InsertBlock(a1); err != nil {
		t.Fatalf("chain A block 1: %v", err)
	}
	a2 := mkBlock(a1, 2, interval)
	if err := bc.InsertBlock(a2); err != nil {
		t.Fatalf("chain A block 2 (maintenance): %v", err)
	}

	// Post-maintenance: active rotated (#27 in, #26 out) and is_jobs flipped.
	if a := active(); a[witnessAddr(26)] || !a[witnessAddr(27)] {
		t.Fatalf("post-maint active not rotated: in26=%v in27=%v, want false/true", a[witnessAddr(26)], a[witnessAddr(27)])
	}
	if isJobs(26) || !isJobs(27) {
		t.Fatalf("post-maint is_jobs not flipped: #26=%v #27=%v, want false/true", isJobs(26), isJobs(27))
	}

	// Chain B from genesis, 3 blocks all strictly below the boundary (never runs
	// maintenance), longer than chain A → switchFork rewinds across the boundary.
	b1 := mkBlock(bc.genesisBlock, 1, 1000)
	b2 := mkBlock(b1, 2, 2000)
	b3 := mkBlock(b2, 3, 3000)
	if err := bc.InsertBlock(b1); err != nil {
		t.Fatalf("chain B block 1: %v", err)
	}
	if err := bc.InsertBlock(b2); err != nil {
		t.Fatalf("chain B block 2: %v", err)
	}
	if bc.CurrentBlock().Hash() != a2.Hash() {
		t.Fatal("chain B should not have switched yet")
	}
	if err := bc.InsertBlock(b3); err != nil {
		t.Fatalf("chain B block 3 (switch trigger): %v", err)
	}
	if bc.CurrentBlock().Hash() != b3.Hash() {
		t.Fatal("switchFork did not move head to chain B tip")
	}

	// Dual rewind: the active list (rooted) AND is_jobs (capsule/buffer) both
	// revert to the pre-maintenance genesis state.
	if a := active(); !a[witnessAddr(26)] || a[witnessAddr(27)] {
		t.Fatalf("post-switchFork active not rewound (root): in26=%v in27=%v, want true/false", a[witnessAddr(26)], a[witnessAddr(27)])
	}
	if !isJobs(26) || isJobs(27) {
		t.Fatalf("post-switchFork is_jobs not rewound (buffer): #26=%v #27=%v, want true/false", isJobs(26), isJobs(27))
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
		// during applyBlock; the flush runs asynchronously on the worker so
		// wait for the queue to drain before observing the buffer state.
		bc.WaitForFlushSettled()
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

	// Read post-applyBlock counters from the restarted head state. Witness
	// capsules are rooted state; the flat witness row is only a compatibility
	// mirror and is not the source of truth.
	w := readWitnessAtHead(t, bc2, witnessAddr)
	if got := w.TotalProduced(); got != 5 {
		t.Fatalf("disk-side TotalProduced = %d, want 5", got)
	}
	if got := w.LatestBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestBlockNum = %d, want 5", got)
	}

	// DP `latest_solidified_block_num` survived restart.
	dpDisk := state.LoadDynamicProperties(diskdb, nil) // derived-only read
	if got := dpDisk.LatestSolidifiedBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestSolidifiedBlockNum = %d, want 5", got)
	}
	if got := dpDisk.LatestBlockHeaderNumber(); got != 5 {
		t.Fatalf("disk-side LatestBlockHeaderNumber = %d, want 5", got)
	}

	// Per-witness latest-block cursor (used by updateSolidifiedBlock) also
	// survives restart via the rooted witness domain.
	if got := readWitnessLatestBlockAtHead(t, bc2, witnessAddr); got != 5 {
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

	w := readWitnessAtHead(t, bc, witnessAddr)
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

	readCycleRewardAtHead := func() int64 {
		t.Helper()
		statedb, err := state.New(bc.HeadStateRoot(), bc.StateDB())
		if err != nil {
			t.Fatalf("open head state: %v", err)
		}
		return statedb.ReadCycleReward(7, witnessAddr.Bytes())
	}

	// Sanity: cycle reward reflects 3 chain-A blocks in the canonical state root.
	gotA := readCycleRewardAtHead()
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
	gotB := readCycleRewardAtHead()
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
		Url:               []byte("https://forktkn.example"),
		FreeAssetNetLimit: 0,
	}
	param, err := anypb.New(tokenContract)
	if err != nil {
		t.Fatal(err)
	}
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
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

	// Sanity: asset indexes are rooted into the head state on chain A.
	if sysKV := bc.sysKVAt(bc.HeadStateRoot()); sysKV == nil {
		t.Fatal("after chain A: could not open head sysKV")
	} else {
		if _, ok := sysKV.ReadAssetNameIndex(assetName); !ok {
			t.Fatal("after chain A: asset name index missing from head state")
		}
		if _, ok := sysKV.ReadAssetOwnerIndex(issuer[:]); !ok {
			t.Fatal("after chain A: asset owner index missing from head state")
		}
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
	// head state after the fork: the new head (chain B tip) rooted no asset,
	// so reading the rooted system-KV at HeadStateRoot() must not find them
	// (solidified=0 means nothing was flushed, so the head root is the
	// canonical source of truth here).
	if sysKV := bc.sysKVAt(bc.HeadStateRoot()); sysKV == nil {
		t.Fatal("after switchFork: could not open head sysKV")
	} else {
		if _, ok := sysKV.ReadAssetNameIndex(assetName); ok {
			t.Fatal("after switchFork: asset name index leaked from orphan branch")
		}
		if _, ok := sysKV.ReadAssetOwnerIndex(issuer[:]); ok {
			t.Fatal("after switchFork: asset owner index leaked from orphan branch")
		}
	}
	// And specifically NOT reachable from disk either: opening the head root
	// against a fresh state.Database (no shared in-memory trie cache) must
	// either fail (head trie never flushed) or, if openable, still not carry
	// the orphaned asset indexes.
	if diskState, err := state.New(bc.HeadStateRoot(), state.NewDatabase(diskdb)); err == nil {
		if _, ok := diskState.ReadAssetNameIndex(assetName); ok {
			t.Fatal("after switchFork: asset name index leaked to disk from orphan branch")
		}
		if _, ok := diskState.ReadAssetOwnerIndex(issuer[:]); ok {
			t.Fatal("after switchFork: asset owner index leaked to disk from orphan branch")
		}
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
	// per applyBlock and the buffer ends up empty once the async worker
	// drains. Close should be a no-op-on-buffer in this configuration
	// (idempotent), so wait for the queue to drain before observing.
	bc.WaitForFlushSettled()
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

	// Rooted witness counters must match what was on the now-flushed buffer.
	w := readWitnessAtHead(t, bc2, witnessAddr)
	if got := w.TotalProduced(); got != 5 {
		t.Fatalf("disk-side TotalProduced = %d, want 5", got)
	}
	if got := w.LatestBlockNum(); got != 5 {
		t.Fatalf("disk-side LatestBlockNum = %d, want 5", got)
	}
	dpDisk := state.LoadDynamicProperties(diskdb, nil) // derived-only read
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
// drop the higher layers. After restart, the canonical head reflects the
// solidified-line state — NOT the unflushed in-memory state.
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
	// because none of the 5 layers were flushed. The persisted head also
	// stays at the flushed state so sync refetches and re-applies the
	// missing range from peers.
	sdb2 := state.NewDatabase(diskdb)
	bc2, err := NewBlockChain(diskdb, sdb2, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := bc2.CurrentBlock().Number(); got != 0 {
		t.Fatalf("restart: head = %d, want 0", got)
	}
	if got := bc2.GetBlockByNumber(5); got != nil {
		t.Fatal("stale block body above recovered head should not be returned as canonical")
	}
	// Witness counter is back at the recovered head root; the dropped layers
	// never became persisted head state.
	if w := readWitnessAtHead(t, bc2, witnessAddr); w.TotalProduced() != 0 {
		t.Fatalf("rooted TotalProduced = %d, want 0 "+
			"(layers above solidified were dropped on Close)", w.TotalProduced())
	}
}

func TestRestartRecoversHeadToLatestFlushedHeader(t *testing.T) {
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

	var block5 *types.Block
	for i := 1; i <= 5; i++ {
		b := buildTestBlock(bc, witnessAddr, int64(i)*3000)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
		if i == 5 {
			block5 = b
		}
	}
	if err := bc.flushBufferUpToSolidified(2); err != nil {
		t.Fatalf("flush up to 2: %v", err)
	}
	if got := state.LoadDynamicProperties(diskdb, nil).LatestBlockHeaderNumber(); got != 2 {
		t.Fatalf("disk latest_block_header_number = %d, want 2", got)
	}

	// Simulate a database produced by the old direct-head write path: the
	// block body and LastBlock point past the latest flushed DP state.
	rawdb.WriteHeadBlockHash(diskdb, block5.Hash())

	sdb2 := state.NewDatabase(diskdb)
	bc2, err := NewBlockChain(diskdb, sdb2, params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := bc2.CurrentBlock().Number(); got != 2 {
		t.Fatalf("restart recovered head = %d, want 2", got)
	}
	headHash := rawdb.ReadHeadBlockHash(diskdb)
	headNum := rawdb.ReadBlockNumber(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), headHash)
	if headNum == nil || *headNum != 2 {
		t.Fatalf("disk LastBlock not repaired: num=%v hash=%x", headNum, headHash)
	}
	if got := bc2.GetBlockByNumber(5); got != nil {
		t.Fatal("stale block body above recovered head should not be returned as canonical")
	}
}
