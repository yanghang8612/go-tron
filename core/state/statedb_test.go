package state

import (
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	return sdb
}

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestStateDBGetSetBalance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)
	if got := sdb.GetBalance(addr); got != 100_000_000 {
		t.Fatalf("balance: want 100000000, got %d", got)
	}
	if err := sdb.SubBalance(addr, 30_000_000); err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetBalance(addr); got != 70_000_000 {
		t.Fatalf("balance after sub: want 70000000, got %d", got)
	}
}

func TestStateDBSubBalanceInsufficient(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 50)
	err := sdb.SubBalance(addr, 100)
	if err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestStateDBSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	snap := sdb.Snapshot()
	sdb.AddBalance(addr, 50)
	if got := sdb.GetBalance(addr); got != 150 {
		t.Fatalf("before revert: want 150, got %d", got)
	}
	sdb.RevertToSnapshot(snap)
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("after revert: want 100, got %d", got)
	}
}

func TestStateDBSnapshotRevertNewAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	snap := sdb.Snapshot()
	addr := testAddr(2)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 200)
	sdb.RevertToSnapshot(snap)
	acc := sdb.GetAccount(addr)
	if acc != nil {
		t.Fatal("account should not exist after revert")
	}
}

func TestStateDBCommitChangesRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	emptyRoot := ethcommon.Hash(ethtypes.EmptyRootHash)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if ethcommon.Hash(root) == emptyRoot {
		t.Fatal("root should differ from empty root after commit")
	}
}

func TestStateDBCommitDeterministic(t *testing.T) {
	makeState := func() tcommon.Hash {
		diskdb := ethrawdb.NewMemoryDatabase()
		db := NewDatabase(diskdb)
		sdb, _ := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
		addr1 := testAddr(1)
		addr2 := testAddr(2)
		sdb.GetOrCreateAccount(addr1)
		sdb.AddBalance(addr1, 500)
		sdb.GetOrCreateAccount(addr2)
		sdb.AddBalance(addr2, 300)
		root, _ := sdb.Commit()
		return root
	}
	root1 := makeState()
	root2 := makeState()
	if root1 != root2 {
		t.Fatalf("roots differ: %x vs %x", root1, root2)
	}
}

func TestStateDBFreezeV2(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 50_000_000)
	acc := sdb.GetAccount(addr)
	if acc == nil {
		t.Fatal("account not found")
	}
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 50_000_000 {
		t.Fatalf("frozen bandwidth: want 50000000, got %d", got)
	}
}

func TestStateDBCommitThenContinue(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)

	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// StateDB should still be usable after commit
	sdb.AddBalance(addr, 500)
	if got := sdb.GetBalance(addr); got != 1500 {
		t.Fatalf("balance after second add: want 1500, got %d", got)
	}

	root2, err := sdb.Commit()
	if err != nil {
		t.Fatal("second commit failed:", err)
	}

	if root1 == root2 {
		t.Fatal("roots should differ after second commit")
	}
}

func TestStateDBWitness(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	w := sdb.GetWitness(addr)
	if w != nil {
		t.Fatal("witness should not exist")
	}
	sdb.GetOrCreateAccount(addr)
	sdb.PutWitness(addr, "http://example.com")
	w = sdb.GetWitness(addr)
	if w == nil {
		t.Fatal("witness should exist")
	}
	if w.URL() != "http://example.com" {
		t.Fatalf("witness url: want http://example.com, got %s", w.URL())
	}
}

func TestStateDBAccountExists(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)

	if db.AccountExists(addr) {
		t.Fatal("should not exist before creation")
	}

	db.CreateAccount(addr, corepb.AccountType_Normal)
	if !db.AccountExists(addr) {
		t.Fatal("should exist after creation")
	}
}

func TestStateDBFreezeAndUnfreeze(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)
	db.AddBalance(addr, 10_000_000)

	// Freeze
	db.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 5_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 5_000_000 {
		t.Fatalf("frozen: got %d, want 5000000", got)
	}
	if got := db.TotalFrozenV2(addr); got != 5_000_000 {
		t.Fatalf("total frozen: got %d, want 5000000", got)
	}

	// Reduce frozen
	db.ReduceFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 3_000_000 {
		t.Fatalf("after reduce: got %d, want 3000000", got)
	}

	// Add unfreeze entry
	db.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000, 100_000)
	if got := db.UnfreezeV2Count(addr); got != 1 {
		t.Fatalf("unfreeze count: got %d, want 1", got)
	}

	// Remove expired
	withdrawn := db.RemoveExpiredUnfreezeV2(addr, 200_000)
	if withdrawn != 2_000_000 {
		t.Fatalf("withdrawn: got %d, want 2000000", withdrawn)
	}
	if got := db.UnfreezeV2Count(addr); got != 0 {
		t.Fatalf("unfreeze count after remove: got %d, want 0", got)
	}
}

func TestStateDBVotes(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	votes := []*corepb.Vote{
		{VoteAddress: testAddr(0x10).Bytes(), VoteCount: 100},
		{VoteAddress: testAddr(0x11).Bytes(), VoteCount: 200},
	}
	db.SetVotes(addr, votes)
	got := db.GetVotes(addr)
	if len(got) != 2 {
		t.Fatalf("votes: got %d, want 2", len(got))
	}

	db.ClearVotes(addr)
	got = db.GetVotes(addr)
	if len(got) != 0 {
		t.Fatalf("after clear: got %d votes, want 0", len(got))
	}
}

func TestStateDBAllowanceAndWithdraw(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	db.SetAllowance(addr, 1000)
	if got := db.GetAllowance(addr); got != 1000 {
		t.Fatalf("allowance: got %d, want 1000", got)
	}

	db.AddAllowance(addr, 500)
	if got := db.GetAllowance(addr); got != 1500 {
		t.Fatalf("after add: got %d, want 1500", got)
	}

	db.SetLatestWithdrawTime(addr, 999)
	if got := db.GetLatestWithdrawTime(addr); got != 999 {
		t.Fatalf("withdraw time: got %d, want 999", got)
	}
}

func TestStateDBWitnessVoteCount(t *testing.T) {
	db := newTestStateDB(t)
	wAddr := testAddr(0x10)
	db.PutWitness(wAddr, "http://w1.example.com")

	db.AddWitnessVoteCount(wAddr, 100)
	w := db.GetWitness(wAddr)
	if w.VoteCount() != 100 {
		t.Fatalf("vote count: got %d, want 100", w.VoteCount())
	}
	db.AddWitnessVoteCount(wAddr, -30)
	if w.VoteCount() != 70 {
		t.Fatalf("after sub: got %d, want 70", w.VoteCount())
	}
}

func TestStateDB_BandwidthMethods(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x01)
	statedb.GetOrCreateAccount(addr)

	// NetUsage
	if statedb.GetNetUsage(addr) != 0 {
		t.Fatal("initial NetUsage should be 0")
	}
	statedb.SetNetUsage(addr, 500)
	if statedb.GetNetUsage(addr) != 500 {
		t.Fatalf("NetUsage: want 500, got %d", statedb.GetNetUsage(addr))
	}

	// LatestConsumeTime
	statedb.SetLatestConsumeTime(addr, 3000)
	if statedb.GetLatestConsumeTime(addr) != 3000 {
		t.Fatalf("LatestConsumeTime: want 3000, got %d", statedb.GetLatestConsumeTime(addr))
	}

	// FreeNetUsage
	statedb.SetFreeNetUsage(addr, 200)
	if statedb.GetFreeNetUsage(addr) != 200 {
		t.Fatalf("FreeNetUsage: want 200, got %d", statedb.GetFreeNetUsage(addr))
	}

	// LatestConsumeFreeTime
	statedb.SetLatestConsumeFreeTime(addr, 6000)
	if statedb.GetLatestConsumeFreeTime(addr) != 6000 {
		t.Fatalf("LatestConsumeFreeTime: want 6000, got %d", statedb.GetLatestConsumeFreeTime(addr))
	}
}

func TestStateDB_BandwidthRevert(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x01)
	statedb.GetOrCreateAccount(addr)
	statedb.SetNetUsage(addr, 100)

	snap := statedb.Snapshot()
	statedb.SetNetUsage(addr, 999)
	if statedb.GetNetUsage(addr) != 999 {
		t.Fatalf("want 999 after set, got %d", statedb.GetNetUsage(addr))
	}

	statedb.RevertToSnapshot(snap)
	if statedb.GetNetUsage(addr) != 100 {
		t.Fatalf("want 100 after revert, got %d", statedb.GetNetUsage(addr))
	}
}
