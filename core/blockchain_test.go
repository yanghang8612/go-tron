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
)

// TestFlipWitnessIsJobs_Rotation locks java-tron MaintenanceManager.applyBlock
// parity (MaintenanceManager.java:135-145): when the active set rotates at a
// maintenance boundary, is_jobs is cleared on every outgoing member and set on
// every incoming member. Members present in both sets end up true.
func TestFlipWitnessIsJobs_Rotation(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	a, b, c := testCoreAddr(1), testCoreAddr(2), testCoreAddr(3)
	for _, addr := range []tcommon.Address{a, b, c} {
		w := types.NewWitness(addr, "")
		w.SetIsJobs(addr == a || addr == b) // a,b are the current active set
		rawdb.WriteWitness(diskdb, addr, w)
	}

	flipWitnessIsJobs(diskdb, []tcommon.Address{a, b}, []tcommon.Address{b, c})

	want := map[tcommon.Address]bool{a: false, b: true, c: true}
	for addr, exp := range want {
		w := rawdb.ReadWitness(diskdb, addr)
		if w == nil {
			t.Fatalf("witness %s missing", addr.Hex())
		}
		if w.IsJobs() != exp {
			t.Errorf("witness %s: IsJobs=%v, want %v", addr.Hex(), w.IsJobs(), exp)
		}
	}
}

// TestFlipWitnessIsJobs_NoChangeSkips verifies the java-tron guard: when the
// active set is unchanged (order-independent), no witness records are touched.
func TestFlipWitnessIsJobs_NoChangeSkips(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	a, b := testCoreAddr(1), testCoreAddr(2)
	for _, addr := range []tcommon.Address{a, b} {
		w := types.NewWitness(addr, "")
		w.SetIsJobs(false) // deliberately stale; guard must leave it untouched
		rawdb.WriteWitness(diskdb, addr, w)
	}

	flipWitnessIsJobs(diskdb, []tcommon.Address{a, b}, []tcommon.Address{b, a})

	for _, addr := range []tcommon.Address{a, b} {
		if w := rawdb.ReadWitness(diskdb, addr); w.IsJobs() {
			t.Errorf("witness %s: IsJobs rewritten despite unchanged set", addr.Hex())
		}
	}
}

func TestNewBlockChain(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
	}

	_, _, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock() == nil {
		t.Fatal("current block should not be nil")
	}
	if bc.CurrentBlock().Number() != 0 {
		t.Fatalf("current block number: want 0, got %d", bc.CurrentBlock().Number())
	}
}

func TestBlockChainInsertBlock(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 99_000_000_000_000_000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block1Header := &corepb.BlockHeaderRaw{
		Number:     1,
		Timestamp:  3000,
		ParentHash: genesisHash[:],
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: block1Header,
		},
	})

	err = bc.InsertBlockWithoutVerify(block1)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block number: want 1, got %d", bc.CurrentBlock().Number())
	}

	stored := rawdb.ReadBlock(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), 1)
	if stored == nil {
		t.Fatal("block 1 not stored")
	}
}

func TestBlockChainGetBlockByNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	block := bc.GetBlockByNumber(0)
	if block == nil {
		t.Fatal("genesis block not found")
	}
}

func TestBlockChainGetBlockByHash(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block := bc.GetBlockByHash(genesisHash)
	if block == nil {
		t.Fatal("genesis block not found by hash")
	}
	if block.Number() != 0 {
		t.Fatalf("expected block number 0, got %d", block.Number())
	}
}

func TestBlockChainInsertInvalidNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Try to insert block with wrong number (2 instead of 1)
	badBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number: 2,
			},
		},
	})

	err := bc.InsertBlockWithoutVerify(badBlock)
	if err != ErrInvalidNumber {
		t.Fatalf("expected ErrInvalidNumber, got %v", err)
	}
}

func TestBlockChainInsertInvalidParent(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Insert block 1 with wrong parent hash
	wrongParent := tcommon.Hash{0xde, 0xad}
	badBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				ParentHash: wrongParent[:],
			},
		},
	})

	err := bc.InsertBlockWithoutVerify(badBlock)
	if err != ErrInvalidParent {
		t.Fatalf("expected ErrInvalidParent, got %v", err)
	}
}

func TestBlockChainActiveWitnesses(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: testCoreAddr(10), VoteCount: 100, URL: "http://w1"},
			{Address: testCoreAddr(11), VoteCount: 200, URL: "http://w2"},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	witnesses := bc.ActiveWitnesses()
	if len(witnesses) == 0 {
		t.Fatal("expected non-empty active witnesses")
	}

	// SetActiveWitnesses now writes through bc.buffer (rewind-safe path), so
	// it must run inside an open buffer layer — mirror applyBlock's
	// BeginBlock/CommitBlock bracket.
	newList := []tcommon.Address{testCoreAddr(20), testCoreAddr(21)}
	bc.buffer.BeginBlock(tcommon.Hash{0x1})
	bc.SetActiveWitnesses(newList)
	bc.buffer.CommitBlock()

	got := bc.ActiveWitnesses()
	if len(got) != 2 || got[0] != testCoreAddr(20) || got[1] != testCoreAddr(21) {
		t.Fatalf("unexpected witnesses after set: %v", got)
	}

	// Persisted through the buffer (not yet flushed to disk): visible via
	// BufferedDB, absent from the bare disk store until a solidified flush.
	persisted := rawdb.ReadActiveWitnesses(bc.BufferedDB())
	if len(persisted) != 2 {
		t.Fatalf("expected 2 buffered witnesses, got %d", len(persisted))
	}
}

func TestBlockChainNextMaintenanceTime(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 1000,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 100000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.NextMaintenanceTime() != 100000 {
		t.Fatalf("expected 100000, got %d", bc.NextMaintenanceTime())
	}
}

func TestBlockChainInsertBlock_Maintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 6000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build block 1 at timestamp 3000 (before maintenance)
	block1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()

	dynProps := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if dynProps.NextMaintenanceTime() != 6000 {
		t.Fatalf("maintenance should not have run yet, got %d", dynProps.NextMaintenanceTime())
	}

	// Build block 2 at timestamp 6000 (at maintenance boundary)
	block2 := buildTestBlock(bc, witnessAddr, 6000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()

	dynProps = loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if dynProps.NextMaintenanceTime() <= 6000 {
		t.Fatalf("next_maintenance_time should have advanced past 6000, got %d", dynProps.NextMaintenanceTime())
	}
}

// TestBlockChainInsertBlock_ProcessProposalsAtMaintenance locks the wiring
// fix that hooks core.ProcessProposals into the per-block maintenance
// boundary in applyBlock. Before this fix the function was defined but
// never called: a Nile soak at h=860k had 4 proposals with 27 SR approvals
// each stuck at `state=PENDING` and `allow_creation_of_contracts=0`,
// keeping every TVM/actuator fork gate disabled forever (2026-05-09).
//
// Setup pre-populates the proposal store with a PENDING proposal that
// would set DP key 9 (allow_creation_of_contracts) to 1, with the sole
// active witness recorded as approver. Crossing the maintenance boundary
// must flip the proposal to APPROVED and apply the DP change.
func TestBlockChainInsertBlock_ProcessProposalsAtMaintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	// Seed a PENDING proposal expiring before the maintenance boundary,
	// approved by the sole active witness (= 100% > 70% threshold).
	pendingProposal := &rawdb.Proposal{
		ID:             1,
		Proposer:       witnessAddr,
		Parameters:     map[int64]int64{9: 1}, // allow_creation_of_contracts
		CreateTime:     0,
		ExpirationTime: interval - 1,
		Approvals:      []tcommon.Address{witnessAddr},
		State:          rawdb.ProposalStatePending,
	}
	if err := rawdb.WriteProposal(diskdb, 1, pendingProposal); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteProposalIndex(diskdb, []int64{1}); err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Block #1 hits the genesis boundary but java-tron skips doMaintenance
	// on block #1 (MaintenanceManager.applyBlock line 63 `if blockNum != 1`).
	// Push one pre-boundary block first so the boundary crossing happens on
	// block #2 where ProcessProposals actually fires.
	preBoundary := buildTestBlock(bc, witnessAddr, 1)
	if err := bc.InsertBlock(preBoundary); err != nil {
		t.Fatal(err)
	}
	block := buildTestBlock(bc, witnessAddr, interval)
	if err := bc.InsertBlock(block); err != nil {
		t.Fatal(err)
	}
	bc.WaitForFlushSettled()

	got := rawdb.ReadProposal(diskdb, 1)
	if got == nil {
		t.Fatal("proposal #1 missing after maintenance")
	}
	if got.State != rawdb.ProposalStateApproved {
		t.Fatalf("proposal #1 state: got %d, want APPROVED (%d)", got.State, rawdb.ProposalStateApproved)
	}
	dp := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if !dp.AllowCreationOfContracts() {
		raw, _ := dp.Get("allow_creation_of_contracts")
		t.Fatalf("allow_creation_of_contracts not set after proposal #1 applied (raw value=%d)", raw)
	}
}

// TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary is the
// regression test for D-2.b — under the original cross-impl fixture
// (CD=OFF) gtron's distributeLegacyStandby fired 37 times in 11 cycles
// (~3.4× over). Even with CD=ON masking the allowance leak, the fix
// must guarantee that crossing a single maintenance boundary triggers
// DoMaintenance exactly once, regardless of how many blocks fall after
// the boundary inside the same maintenance interval.
func TestBlockChainInsertBlock_MaintenanceFiresOncePerBoundary(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000) // 6h, java-tron default
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval, // first boundary at t=interval
		},
	}

	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	var fires int
	bc.AddMaintenanceHook(func(*types.Block, []tcommon.Address) {
		fires++
	})

	// Push a pre-boundary block #1 first so the boundary crossings below
	// land on block #2+. java-tron skips doMaintenance on block #1
	// regardless of `flag`, so feeding the boundary on block #1 would
	// register zero fires and conflate two distinct behaviors.
	preBoundary := buildTestBlock(bc, witnessAddr, 1)
	if err := bc.InsertBlock(preBoundary); err != nil {
		t.Fatalf("InsertBlock(preBoundary): %v", err)
	}

	// Three blocks all *after* the first boundary but inside the same
	// interval. Only the first should trigger maintenance; the next two
	// must observe the advanced next_maintenance_time and skip.
	timestamps := []int64{interval, interval + 3000, interval + 6000}
	for _, ts := range timestamps {
		block := buildTestBlock(bc, witnessAddr, ts)
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("InsertBlock(ts=%d): %v", ts, err)
		}
	}

	if fires != 1 {
		t.Fatalf("DoMaintenance fires across one boundary: got %d, want 1", fires)
	}

	// next_maintenance_time must advance to exactly 2*interval after one
	// fire (round=0 in CalcNextMaintenanceTime, since blockTime − currentMaint
	// < interval).
	dynProps := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if got, want := dynProps.NextMaintenanceTime(), 2*interval; got != want {
		t.Fatalf("next_maintenance_time after fire: got %d, want %d", got, want)
	}

	// Now feed a block that crosses the *second* boundary — exactly one
	// more fire. Confirms multi-boundary cadence.
	block := buildTestBlock(bc, witnessAddr, 2*interval+1000)
	if err := bc.InsertBlock(block); err != nil {
		t.Fatal(err)
	}
	if fires != 2 {
		t.Fatalf("DoMaintenance fires across two boundaries: got %d, want 2", fires)
	}

	// Long stress: feed blocks every 3s for several maintenance intervals.
	// Mirrors the cross-impl scenario where many blocks fall between
	// maintenance boundaries. Trigger must fire exactly once per boundary.
	startBlockNum := bc.CurrentBlock().Number()
	startTs := bc.CurrentBlock().Timestamp()
	const blockTickMs = int64(3000)
	const cycles = int64(5) // five maintenance cycles, ~36k blocks at 3s/block
	want := int(2 + cycles)
	for ts := startTs + blockTickMs; ts <= startTs+cycles*interval+blockTickMs; ts += blockTickMs {
		b := buildTestBlock(bc, witnessAddr, ts)
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("InsertBlock at ts=%d: %v", ts, err)
		}
	}
	if fires != want {
		t.Fatalf("DoMaintenance fires across stress run: got %d, want %d (blocks=%d→%d)",
			fires, want, startBlockNum+1, bc.CurrentBlock().Number())
	}
}

// TestBlockChainInsertBlock_RootedDynPropsRewind is the Phase 3b pipeline
// acceptance gate: a maintenance block changes a rooted dynprop
// (next_maintenance_time), which must (a) move the internal full-state root
// (anchor) and (b) remain recoverable by reopening the OLD root after the chain
// advances (rewind). This is the whole point of rooting dynamic properties.
func TestBlockChainInsertBlock_RootedDynPropsRewind(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Block #1 (ts=1): pre-boundary, next_maintenance_time stays = interval.
	if err := bc.InsertBlock(buildTestBlock(bc, witnessAddr, 1)); err != nil {
		t.Fatalf("InsertBlock(#1): %v", err)
	}
	bc.WaitForFlushSettled()
	rootBefore := bc.HeadStateRoot()
	if got := loadDPAtRoot(t, diskdb, sdb, rootBefore).NextMaintenanceTime(); got != interval {
		t.Fatalf("pre-boundary next_maintenance_time: got %d, want %d", got, interval)
	}

	// Block #2 (ts=interval): crosses the boundary; next_maintenance_time
	// advances to 2*interval and the rooted change moves the state root.
	if err := bc.InsertBlock(buildTestBlock(bc, witnessAddr, interval)); err != nil {
		t.Fatalf("InsertBlock(#2): %v", err)
	}
	bc.WaitForFlushSettled()
	rootAfter := bc.HeadStateRoot()

	if rootBefore == rootAfter {
		t.Fatal("anchor: maintenance changed next_maintenance_time but the state root did not move")
	}
	if got := loadDPAtRoot(t, diskdb, sdb, rootAfter).NextMaintenanceTime(); got != 2*interval {
		t.Fatalf("post-boundary next_maintenance_time at head root: got %d, want %d", got, 2*interval)
	}

	// Rewind: reopening the pre-boundary root must still yield the OLD value,
	// even though the chain has advanced past it.
	if got := loadDPAtRoot(t, diskdb, sdb, rootBefore).NextMaintenanceTime(); got != interval {
		t.Fatalf("rewind to pre-boundary root: next_maintenance_time = %d, want %d", got, interval)
	}
}

// TestBlockChainInsertBlock_Block1SkipsMaintenance locks the java-tron
// MaintenanceManager.applyBlock contract (lines 62-75): when block #1
// crosses the genesis-seeded boundary, the chain still advances
// next_maintenance_time per `updateNextMaintenanceTime(blockTime)` but
// SKIPS doMaintenance entirely — no legacy standby allowance is paid, no
// active-set rotation, no proposal processing, no cycle 0 VI
// accumulation. This is why Nile's deployed mainnet keeps the GR set
// intact on block #1 and runs its first real maintenance on block #2+.
//
// Without this skip, gtron paid `witness_standby_allowance` to GR
// witnesses on block #1 (and rotated them off the active set), creating
// state-root divergence on the very first block of any Nile bootstrap.
//
// The genesis-seeded boundary fixture uses Nile-like inputs: Timestamp=0,
// MaintenanceTimeInterval=21_600_000, NextMaintenanceTime=21_600_000.
// Block #1 lands at a real Nile-era timestamp (1572408000000 = Oct 30
// 2019 03:20 UTC). java-tron's updateNextMaintenanceTime formula yields
// 1572415200000 (Oct 30 06:00 UTC) — currentMaint + (round+1)*interval
// with round = (blockTime - currentMaint) / interval = 72795.
func TestBlockChainInsertBlock_Block1SkipsMaintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	const block1Time = int64(1_572_408_000_000) // Oct 30 2019 03:20 UTC
	// java's updateNextMaintenanceTime: currentMaint=21_600_000,
	// blockTime=1_572_408_000_000, interval=21_600_000
	// → round = (1572408000000 - 21600000) / 21600000 = 72795
	// → next = 21600000 + 72796*21600000 = 1572415200000.
	const wantNextMaint = int64(1_572_415_200_000)

	const standbyAllowance = int64(115_200_000_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval": interval,
			"next_maintenance_time":     interval,
			// CD=OFF so distributeLegacyStandby would pay allowance — if the
			// skip is missing, this witness's allowance will jump by
			// standby_allowance × (votes / total_votes) = standbyAllowance.
			"witness_standby_allowance": standbyAllowance,
			"change_delegation":         0,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	// Seed a PENDING proposal that would APPROVE at the boundary if
	// ProcessProposals ran. Skip must keep it PENDING.
	pendingProposal := &rawdb.Proposal{
		ID:             1,
		Proposer:       witnessAddr,
		Parameters:     map[int64]int64{9: 1}, // allow_creation_of_contracts
		CreateTime:     0,
		ExpirationTime: block1Time - 1,
		Approvals:      []tcommon.Address{witnessAddr},
		State:          rawdb.ProposalStatePending,
	}
	if err := rawdb.WriteProposal(diskdb, 1, pendingProposal); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteProposalIndex(diskdb, []int64{1}); err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	var maintFires int
	bc.AddMaintenanceHook(func(*types.Block, []tcommon.Address) {
		maintFires++
	})

	block1 := buildTestBlock(bc, witnessAddr, block1Time)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock(block#1): %v", err)
	}
	bc.WaitForFlushSettled()

	// 1. Grid still advances per java's updateNextMaintenanceTime formula.
	dp := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if got := dp.NextMaintenanceTime(); got != wantNextMaint {
		t.Fatalf("next_maintenance_time after block #1: got %d, want %d (java formula output)", got, wantNextMaint)
	}

	// 2. State flag is still set (java line 76 sets it from `flag` regardless
	//    of blockNum).
	if got := dp.StateFlag(); got != 1 {
		t.Fatalf("state_flag after block #1 boundary: got %d, want 1", got)
	}

	// 3. Maintenance hook MUST NOT fire — java skips srPrePrepare for
	//    blockNum==1 (line 70 guard).
	if maintFires != 0 {
		t.Fatalf("maintenance hook fires on block #1: got %d, want 0", maintFires)
	}

	// 4. Legacy standby allowance did NOT pay out. With CD=OFF, sole-witness
	//    distribution would credit ~standbyAllowance to witnessAddr's
	//    allowance. Block reward also accrues, so the strict invariant is
	//    "allowance < standbyAllowance" (block reward is 16M sun, well under
	//    115.2G).
	stateRoot := rawdb.ReadBlockStateRoot(rawdb.NewChainDB(diskdb, rawdb.NoopAncient{}), bc.CurrentBlock().Hash())
	statedb, err := state.New(stateRoot, sdb)
	if err != nil {
		t.Fatalf("open post-block#1 state: %v", err)
	}
	if got := statedb.GetAllowance(witnessAddr); got >= standbyAllowance {
		t.Fatalf("witness allowance after block #1: got %d, want < %d (block reward only, no standby payout)", got, standbyAllowance)
	}

	// 5. Pending proposal stays pending (ProcessProposals skipped).
	gotProp := rawdb.ReadProposal(diskdb, 1)
	if gotProp == nil {
		t.Fatal("proposal #1 missing")
	}
	if gotProp.State != rawdb.ProposalStatePending {
		t.Fatalf("proposal #1 state after block #1: got %d, want PENDING (%d)", gotProp.State, rawdb.ProposalStatePending)
	}
	dpAfter := loadDPAtRoot(t, diskdb, bc.StateDB(), bc.HeadStateRoot())
	if dpAfter.AllowCreationOfContracts() {
		t.Fatal("allow_creation_of_contracts unexpectedly applied — ProcessProposals fired on block #1")
	}
}

func TestSolidifiedBlockSingleSR(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://sr1"},
		},
		DynamicProperties: map[string]int64{
			// Push maintenance far out so it doesn't fire during the test.
			"next_maintenance_time": 9_000_000_000,
		},
	}

	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Single SR: floor(1 * 0.3) = 0, so solidified == that SR's latest block.
	const numBlocks = 5
	for i := 1; i <= numBlocks; i++ {
		block := buildTestBlock(bc, witnessAddr, int64(i*3000))
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}
	bc.WaitForFlushSettled()

	want := uint64(numBlocks)
	got := uint64(state.LoadDynamicProperties(diskdb, nil).LatestSolidifiedBlockNum()) // derived-only
	if got != want {
		t.Fatalf("LatestSolidifiedBlockNum: got %d, want %d", got, want)
	}

	// Also confirm it matches the current head.
	if bc.CurrentBlock().Number() != want {
		t.Fatalf("CurrentBlock.Number: got %d, want %d", bc.CurrentBlock().Number(), want)
	}
}

// TestBlockChainInsertBlock_TryRemoveThePowerOfTheGr exercises the full path:
// crossing a maintenance boundary with REMOVE_THE_POWER_OF_THE_GR=1 strips
// the GR's initial vote and clears the flag to -1. Mirrors java-tron
// MaintenanceManager.tryRemoveThePowerOfTheGr (consensus/.../dpos/Maintenance
// Manager.java:194-204).
func TestBlockChainInsertBlock_TryRemoveThePowerOfTheGr(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	grAddr := testCoreAddr(10)
	const interval = int64(21_600_000)
	const initialGRVote = int64(100_000_000)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: grAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: grAddr, VoteCount: initialGRVote, URL: "http://gr1"},
		},
		DynamicProperties: map[string]int64{
			"maintenance_time_interval":  interval,
			"next_maintenance_time":      interval,
			"remove_the_power_of_the_gr": 1,
		},
	}
	if _, _, err := SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Block #1 pre-boundary (java-tron skips doMaintenance for block #1, so
	// the boundary block must land at block #2+ for tryRemoveThePowerOfTheGr
	// to actually fire).
	if err := bc.InsertBlock(buildTestBlock(bc, grAddr, interval/2)); err != nil {
		t.Fatal(err)
	}

	// Block #2 crosses the maintenance boundary.
	if err := bc.InsertBlock(buildTestBlock(bc, grAddr, interval)); err != nil {
		t.Fatal(err)
	}

	w := rawdb.ReadWitness(bc.BufferedDB(), grAddr)
	if w == nil {
		t.Fatal("GR witness missing after maintenance")
	}
	if got := w.VoteCount(); got != 0 {
		t.Fatalf("GR voteCount after strip: got %d, want 0 (100M − 100M)", got)
	}

	dp := loadDPAtRoot(t, bc.BufferedDB(), bc.StateDB(), bc.HeadStateRoot())
	if got := dp.RemoveThePowerOfTheGr(); got != -1 {
		t.Fatalf("flag after strip: got %d, want -1", got)
	}

	// Second maintenance boundary: flag is -1, GR vote must stay at 0 (no
	// further strip), confirming the one-shot guard.
	if err := bc.InsertBlock(buildTestBlock(bc, grAddr, 2*interval)); err != nil {
		t.Fatal(err)
	}
	w2 := rawdb.ReadWitness(bc.BufferedDB(), grAddr)
	if got := w2.VoteCount(); got != 0 {
		t.Fatalf("GR voteCount after second maintenance: got %d, want 0", got)
	}
	if got := loadDPAtRoot(t, bc.BufferedDB(), bc.StateDB(), bc.HeadStateRoot()).RemoveThePowerOfTheGr(); got != -1 {
		t.Fatalf("flag after second maintenance: got %d, want -1", got)
	}
}

func buildTestBlock(bc *BlockChain, witnessAddr tcommon.Address, timestamp int64) *types.Block {
	parent := bc.CurrentBlock()
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         int64(parent.Number() + 1),
				Timestamp:      timestamp,
				ParentHash:     parent.Hash().Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
}
