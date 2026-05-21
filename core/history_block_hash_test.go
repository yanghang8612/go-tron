package core

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestProposalAllowTvmPragueDeploysHistoryBlockHash(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb, err := state.New(tcommon.Hash{}, state.NewDatabase(db))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	dp := state.NewDynamicProperties()
	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}}
	p := &rawdb.Proposal{
		ID:             1,
		Parameters:     map[int64]int64{95: 1},
		ExpirationTime: 1000,
		Approvals:      active,
		State:          rawdb.ProposalStatePending,
	}
	if err := rawdb.WriteProposal(db, p.ID, p); err != nil {
		t.Fatalf("WriteProposal: %v", err)
	}
	if err := rawdb.WriteProposalIndex(db, []int64{p.ID}); err != nil {
		t.Fatalf("WriteProposalIndex: %v", err)
	}

	if err := ProcessProposals(db, dp, active, 1001, nil, statedb); err != nil {
		t.Fatalf("ProcessProposals: %v", err)
	}

	if !dp.AllowTvmPrague() {
		t.Fatal("allow_tvm_prague should be enabled")
	}
	if !dp.BlockHashHistoryInstalled() {
		t.Fatal("block_hash_history_installed should be enabled")
	}
	if code := statedb.GetCode(historyStorageAddress); !bytes.Equal(code, historyStorageCode) {
		t.Fatalf("history code mismatch: got %x", code)
	}
	acct := statedb.GetAccount(historyStorageAddress)
	if acct == nil {
		t.Fatal("history account not created")
	}
	if acct.Type() != corepb.AccountType_Contract {
		t.Fatalf("history account type: got %v", acct.Type())
	}
	if acct.AccountName() != historyStorageName {
		t.Fatalf("history account name: got %q", acct.AccountName())
	}
	meta := statedb.GetContract(historyStorageAddress)
	if meta == nil {
		t.Fatal("history contract metadata not created")
	}
	if meta.Name != historyStorageName || meta.ConsumeUserResourcePercent != 100 {
		t.Fatalf("history contract metadata mismatch: %+v", meta)
	}
}

func TestWriteHistoryBlockHashStoresParentHashBySlot(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb, err := state.New(tcommon.Hash{}, state.NewDatabase(db))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	dp := state.NewDynamicProperties()
	dp.SetBlockHashHistoryInstalled(true)
	parentHash := tcommon.HexToHash("1234")

	writeHistoryBlockHash(statedb, dp, historyServeWindow+1, parentHash)

	if got := statedb.GetState(historyStorageAddress, uint64ToDataWord(0)); got != parentHash {
		t.Fatalf("history slot mismatch: got %s want %s", got.Hex(), parentHash.Hex())
	}
}

// TestWriteHistoryBlockHashCapturesRingPreValue is the regression test for
// the State History Index ring-wrap bug. writeHistoryBlockHash calls
// SetState directly without going through opSload, so without an internal
// pre-warm the journal's storageChange.prev captures zero rather than the
// real disk value. Once the 8191-block ring wraps (block ≥ 8192), the
// sh-s- row at the wrapped slot would record zero instead of the
// prior-cycle parent hash.
//
// Setup mirrors the wrap condition:
//
//  1. deployHistoryBlockHash + Commit installs the contract account on disk
//     (without it, GetState would short-circuit on a nil stateObject and the
//     pre-warm path would never reach the storage disk fallback).
//  2. Plant a known non-zero value at slot 0 directly through rawdb —
//     stand-in for "block 1's parent hash that we wrote 8191 blocks ago",
//     with the statedb's storage cache empty afterwards.
//  3. Drive writeHistoryBlockHash for blockNum = historyServeWindow + 1 so
//     the slot index wraps back to 0.
//  4. Flush via AccumulateHistory and assert the recorded pre-value is the
//     planted priorHash, not zero.
//
// FAILS on pre-fix code (got 0x000…0), PASSES on fixed code (got priorHash).
func TestWriteHistoryBlockHashCapturesRingPreValue(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	stateDatabase := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, stateDatabase)
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	dp := state.NewDynamicProperties()

	// Install the BlockHashHistory contract account so GetState's disk
	// fallback actually fires. Without this, getStateObject returns nil
	// and the pre-warm becomes a no-op for the wrong reason.
	deployHistoryBlockHash(diskdb, statedb, dp)
	root, err := statedb.Commit()
	if err != nil {
		t.Fatalf("seed Commit: %v", err)
	}
	// Re-open statedb from disk: this clears in-memory caches (including
	// any nascent storage entry) so a subsequent GetState is forced to
	// read from rawdb, exactly mimicking the post-cycle ring-wrap state
	// where the contract account is loaded fresh and the slot has been
	// living on disk since the previous cycle.
	statedb, err = state.New(root, stateDatabase)
	if err != nil {
		t.Fatalf("state.New post-commit: %v", err)
	}

	slotKey := uint64ToDataWord(0)
	priorHash := tcommon.HexToHash("cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe")
	statedb.SetState(historyStorageAddress, slotKey, priorHash)
	root, err = statedb.Commit()
	if err != nil {
		t.Fatalf("seed ring slot Commit: %v", err)
	}
	statedb, err = state.New(root, stateDatabase)
	if err != nil {
		t.Fatalf("state.New post-ring-seed: %v", err)
	}

	statedb.SetHistoryEnabled(true)
	newParentHash := tcommon.HexToHash("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	// Block number = ring + 1 → slot index wraps to 0.
	blockNum := historyServeWindow + 1
	writeHistoryBlockHash(statedb, dp, blockNum, newParentHash)

	buf := ethrawdb.NewMemoryDatabase()
	if err := statedb.AccumulateHistory(buf, blockNum, tcommon.Hash{0xAB}); err != nil {
		t.Fatalf("AccumulateHistory: %v", err)
	}

	got, ok := rawdb.ReadSlotDelta(buf, blockNum, historyStorageAddress, slotKey)
	if !ok {
		t.Fatal("SlotDelta missing for ring slot")
	}
	if got != priorHash {
		t.Fatalf("ring slot pre-value: got %s, want %s (real prior-cycle hash, not zero)",
			got.Hex(), priorHash.Hex())
	}
}
