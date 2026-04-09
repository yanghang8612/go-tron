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
