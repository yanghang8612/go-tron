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
