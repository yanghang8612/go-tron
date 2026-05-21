package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountKVSetGet(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k1"))
	if err != nil || !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("get = (%q,%v,%v), want (v1,true,nil)", got, ok, err)
	}
}

func TestAccountKVDomainIsolation(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("a"))
	_ = sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("k"), []byte("b"))
	g1, _, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"))
	g2, _, _ := sdb.GetAccountKV(addr, kvdomains.ContractStorage, []byte("k"))
	if !bytes.Equal(g1, []byte("a")) || !bytes.Equal(g2, []byte("b")) {
		t.Fatalf("domain isolation broken: %q %q", g1, g2)
	}
}

func TestAccountKVDelete(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v"))
	if err := sdb.DeleteAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); ok {
		t.Fatal("key should be absent after delete")
	}
}

func TestAccountKVUnregisteredDomain(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	if err := sdb.SetAccountKV(addr, kvdomains.KVDomain(0x0099), []byte("k"), []byte("v")); err == nil {
		t.Fatal("set with unregistered domain must error")
	}
}

func TestAccountKVSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v1"))
	snap := sdb.Snapshot()
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v2"))
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k2"), []byte("x"))
	sdb.RevertToSnapshot(snap)
	if g, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); !ok || !bytes.Equal(g, []byte("v1")) {
		t.Fatalf("k after revert = %q, want v1", g)
	}
	if _, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k2")); ok {
		t.Fatal("k2 should be gone after revert")
	}
}

func TestAccountKVRootMovesAndPersists(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	root0, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit0: %v", err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit1: %v", err)
	}
	if root1 == root0 {
		t.Fatal("KV write did not move the full state root")
	}
	reopened, err := New(root1, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if g, ok, _ := reopened.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); !ok || string(g) != "v" {
		t.Fatalf("persisted get = %q,%v, want v,true", g, ok)
	}
}

func TestAccountKVDeterministicRoot(t *testing.T) {
	build := func() tcommon.Hash {
		sdb := newTestStateDB(t)
		addr := testAddr(0x22)
		sdb.CreateAccount(addr, corepb.AccountType_Normal)
		_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("a"), []byte("1"))
		_ = sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("b"), []byte("2"))
		_ = sdb.SetAccountKV(addr, kvdomains.SystemProposal, []byte("c"), []byte("3"))
		r, err := sdb.Commit()
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		return r
	}
	if build() != build() {
		t.Fatal("KV commit is non-deterministic")
	}
}

func TestAccountKVEmptyValueDistinctFromDeleted(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x33)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("empty"), []byte{})
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, _ := New(root, sdb.db)
	v, ok, _ := reopened.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("empty"))
	if !ok || len(v) != 0 {
		t.Fatalf("empty-but-present value lost: v=%q ok=%v", v, ok)
	}
}

func TestBalanceOnlyAccountKeepsEmptyKVRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x44)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 5)
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	obj := sdb.getStateObject(addr)
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("balance-only account got non-empty KV root %x", obj.accountKVRoot)
	}
}

func TestResetAccountKVBumpsGenerationAndEmptiesRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x55)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v"))
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := sdb.ResetAccountKV(addr); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit2: %v", err)
	}
	obj := sdb.getStateObject(addr)
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("KV root after reset = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 1 {
		t.Fatalf("generation after reset = %d, want 1", obj.accountKVGeneration)
	}
	if _, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); ok {
		t.Fatal("key should be unreachable after reset+commit")
	}
}

func TestResetAccountKVRevertRestoresOverlay(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x66)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("orig"))
	snap := sdb.Snapshot()
	_ = sdb.ResetAccountKV(addr)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k2"), []byte("new"))
	sdb.RevertToSnapshot(snap)
	if g, ok, _ := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); !ok || string(g) != "orig" {
		t.Fatalf("k after revert-past-reset = %q,%v, want orig,true", g, ok)
	}
	if obj := sdb.getStateObject(addr); obj.accountKVGeneration != 0 {
		t.Fatalf("generation after revert = %d, want 0", obj.accountKVGeneration)
	}
}
