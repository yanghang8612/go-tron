package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/params"
)

func TestCommitmentBranchRotationKeepsLiveDeltaAcrossSnapshotPublish(t *testing.T) {
	witness := testInsertAddr(0xc1)
	db := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: witness, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witness, VoteCount: 1, URL: "test"},
		},
		DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1},
	}
	if _, _, err := SetupGenesisBlock(db, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(db, state.NewDatabase(db), params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bc.Close() }()

	block1 := buildTestBlock(bc, witness, 3_000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if err := rawdb.WriteStateTxRange(db, block1.Number(), block1.Hash(), 1, 1); err != nil {
		t.Fatal(err)
	}
	rotation, rotating, err := bc.BeginCommitmentBranchRotation()
	if err != nil || !rotating {
		t.Fatalf("begin rotation rotating=%v err=%v", rotating, err)
	}
	if rotation.BlockNum != block1.Number() || rotation.BlockHash != block1.Hash() {
		t.Fatalf("rotation boundary = %d/%x, want %d/%x", rotation.BlockNum, rotation.BlockHash, block1.Number(), block1.Hash())
	}
	// Restart in the first crash window: the durable rotation marker must be
	// sufficient to recover delta->legacy reads without any snapshot files.
	if err := bc.Close(); err != nil {
		t.Fatal(err)
	}
	bc, err = NewBlockChain(db, state.NewDatabase(db), params.MainnetChainConfig)
	if err != nil {
		t.Fatalf("restart during rotation: %v", err)
	}
	if resumed, ok, err := rawdb.ReadCommitmentBranchRotation(db); err != nil || !ok || resumed != rotation {
		t.Fatalf("resumed rotation = %+v ok=%v err=%v", resumed, ok, err)
	}

	// Import continues while the immutable builder streams the frozen legacy
	// table. Empty blocks still update witness/account state and therefore the
	// staged commitment, exercising real delta writes.
	block2 := buildTestBlock(bc, witness, 6_000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	liveRoot, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil || !ok {
		t.Fatalf("read live root ok=%v err=%v", ok, err)
	}
	if liveRoot == rotation.Root {
		t.Fatal("post-rotation block did not advance commitment root")
	}

	dir := t.TempDir()
	result, err := snapshots.NewAggregator(dir).BuildLatest(db, snapshots.AggregatorBuildOptions{
		FromTxNum: 1, ToTxNum: rotation.SnapshotTxNum,
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := snapshots.OpenPinnedManager(dir, result.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := bc.CompleteCommitmentBranchRotation(rotation, mgr); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rawdb.ReadCommitmentBranchRotation(db); err != nil || ok {
		t.Fatalf("rotation marker after publish ok=%v err=%v", ok, err)
	}
	base, ok, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil || !ok || base.Generation != rotation.Generation || base.Root != rotation.Root {
		t.Fatalf("base after publish = %+v ok=%v err=%v", base, ok, err)
	}
	legacyRows := 0
	if err := rawdb.IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		legacyRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 0 {
		t.Fatalf("legacy rows after publish = %d", legacyRows)
	}
	delta, err := rawdb.NewCommitmentBranchDeltaKeyspace(rotation.Generation)
	if err != nil {
		t.Fatal(err)
	}
	deltaRows := 0
	if err := delta.Iterate(db, func(_, _ []byte) (bool, error) {
		deltaRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if deltaRows == 0 {
		t.Fatal("live updates produced no branch delta rows")
	}
	// Model a crash after the base marker became durable but before legacy
	// cleanup finished. Even a conflicting leftover legacy root must be ignored.
	var staleLegacyRoot statedomains.BranchData
	staleLegacyRoot.SetHashChild(0, rotation.Root)
	if err := rawdb.WriteCommitmentBranch(db, nil, staleLegacyRoot.Encode()); err != nil {
		t.Fatal(err)
	}

	latest, err := statedomains.NewStagedCommitmentStoreWithRepair(db, statedomains.CommitmentSnapshotRepair{Source: mgr}, false)
	if err != nil {
		t.Fatal(err)
	}
	present, err := latest.RootNodePresent(liveRoot)
	if err != nil || !present {
		t.Fatalf("published snapshot+delta root present=%v err=%v", present, err)
	}
	if err := statedomains.CloseLatestCommitmentStore(latest); err != nil {
		t.Fatal(err)
	}
	if _, active, err := bc.BeginCommitmentBranchRotation(); err != nil || active {
		t.Fatalf("resume base cleanup active=%v err=%v", active, err)
	}
	legacyRows = 0
	if err := rawdb.IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		legacyRows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 0 {
		t.Fatalf("resumed base cleanup left %d legacy rows", legacyRows)
	}
}
