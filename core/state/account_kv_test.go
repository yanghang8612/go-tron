package state

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

func TestLoadAccountReferencePreservesAccountKVRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x12)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(addr, kvdomains.WitnessCapsule, []byte("witness"), []byte("rooted")); err != nil {
		t.Fatalf("set kv: %v", err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	source, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	acc := source.AccountReference(addr)
	if acc == nil {
		t.Fatal("account reference missing")
	}
	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reloaded.LoadAccountReference(acc)
	got, ok, err := reloaded.GetAccountKV(addr, kvdomains.WitnessCapsule, []byte("witness"))
	if err != nil || !ok || string(got) != "rooted" {
		t.Fatalf("loaded account lost KV root: got %q ok=%v err=%v", got, ok, err)
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

func TestAccountKVTrieCacheInvalidatedAcrossResetRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x65)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k"), []byte("orig"))
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	sdb, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); err != nil || !ok || string(got) != "orig" {
		t.Fatalf("warm cache get = %q ok=%v err=%v", got, ok, err)
	}
	snap := sdb.Snapshot()
	if err := sdb.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); err != nil || ok {
		t.Fatalf("reset key visible through stale trie cache: ok=%v err=%v", ok, err)
	}
	sdb.RevertToSnapshot(snap)
	if got, ok, err := sdb.GetAccountKV(addr, kvdomains.SystemDynamicProperty, []byte("k")); err != nil || !ok || string(got) != "orig" {
		t.Fatalf("reverted get = %q ok=%v err=%v, want orig,true,nil", got, ok, err)
	}
}

func TestAccountKVLatestIndexCommitDeleteAndIterate(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x67)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDelegation, []byte("aa/2"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDelegation, []byte("aa/1"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	got, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.SystemDelegation, []byte("aa/1"))
	if err != nil || !ok || string(got) != "1" {
		t.Fatalf("latest index = %q ok=%v err=%v, want 1,true,nil", got, ok, err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	err = reopened.IterateAccountKV(addr, kvdomains.SystemDelegation, []byte("aa/"), func(key, value []byte) (bool, error) {
		keys = append(keys, string(key)+"="+string(value))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"aa/1=1", "aa/2=2"}; !equalStringSlices(keys, want) {
		t.Fatalf("iteration = %v, want %v", keys, want)
	}

	if err := reopened.DeleteAccountKV(addr, kvdomains.SystemDelegation, []byte("aa/1")); err != nil {
		t.Fatal(err)
	}
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.SystemDelegation, []byte("aa/1")); err != nil || ok {
		t.Fatalf("deleted latest index ok=%v err=%v", ok, err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reopened.GetAccountKV(addr, kvdomains.SystemDelegation, []byte("aa/1")); err != nil || ok {
		t.Fatalf("deleted MPT value ok=%v err=%v", ok, err)
	}
}

func TestAccountKVIterateMergesDirtyOverlay(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x68)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemMarket, []byte("p/1"), []byte("old"))
	_ = sdb.SetAccountKV(addr, kvdomains.SystemMarket, []byte("p/2"), []byte("keep"))
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	_ = reopened.SetAccountKV(addr, kvdomains.SystemMarket, []byte("p/1"), []byte("new"))
	_ = reopened.SetAccountKV(addr, kvdomains.SystemMarket, []byte("p/3"), []byte("overlay"))
	_ = reopened.DeleteAccountKV(addr, kvdomains.SystemMarket, []byte("p/2"))

	var keys []string
	err = reopened.IterateAccountKV(addr, kvdomains.SystemMarket, []byte("p/"), func(key, value []byte) (bool, error) {
		keys = append(keys, string(key)+"="+string(value))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"p/1=new", "p/3=overlay"}; !equalStringSlices(keys, want) {
		t.Fatalf("merged iteration = %v, want %v", keys, want)
	}
}

func TestDeleteAccountKVPrefixUsesLatestIndex(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x69)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("cycle/1"), []byte("1"))
	_ = sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("cycle/2"), []byte("2"))
	_ = sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("other"), []byte("x"))
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.DeleteAccountKVPrefix(addr, kvdomains.SystemReward, []byte("cycle/")); err != nil {
		t.Fatal(err)
	}
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("cycle/1"), []byte("cycle/2")} {
		if _, ok, err := reopened.GetAccountKV(addr, kvdomains.SystemReward, key); err != nil || ok {
			t.Fatalf("%s survived prefix delete: ok=%v err=%v", key, ok, err)
		}
		if _, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.SystemReward, key); err != nil || ok {
			t.Fatalf("%s survived latest prefix delete: ok=%v err=%v", key, ok, err)
		}
	}
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.SystemReward, []byte("other")); err != nil || !ok || string(got) != "x" {
		t.Fatalf("other = %q ok=%v err=%v", got, ok, err)
	}
}

func TestResetAccountKVLeavesOldLatestGenerationUnreachable(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x6a)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("slot"), []byte("old"))
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	_ = reopened.SetAccountKV(addr, kvdomains.ContractStorage, []byte("slot"), []byte("new"))
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}

	if old, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.ContractStorage, []byte("slot")); err != nil || !ok || string(old) != "old" {
		t.Fatalf("old generation latest = %q ok=%v err=%v", old, ok, err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := reopened.IterateAccountKV(addr, kvdomains.ContractStorage, []byte("slot"), func(key, value []byte) (bool, error) {
		keys = append(keys, string(key)+"="+string(value))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"slot=new"}; !equalStringSlices(keys, want) {
		t.Fatalf("generation-scoped iteration = %v, want %v", keys, want)
	}
}

func TestRecreatedAccountUsesNextKVGeneration(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x6b)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	_ = sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("slot"), []byte("old"))
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reopened.DeleteAccount(addr)
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	reopened.CreateAccount(addr, corepb.AccountType_Normal)
	_ = reopened.SetAccountKV(addr, kvdomains.ContractStorage, []byte("slot"), []byte("new"))
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	obj := reopened.getStateObject(addr)
	if obj.accountKVGeneration != 1 {
		t.Fatalf("recreated generation = %d, want 1", obj.accountKVGeneration)
	}
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.ContractStorage, []byte("slot")); err != nil || !ok || string(got) != "new" {
		t.Fatalf("recreated slot = %q ok=%v err=%v", got, ok, err)
	}
	var keys []string
	if err := reopened.IterateAccountKV(addr, kvdomains.ContractStorage, []byte("slot"), func(key, value []byte) (bool, error) {
		keys = append(keys, string(key)+"="+string(value))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"slot=new"}; !equalStringSlices(keys, want) {
		t.Fatalf("recreated iteration = %v, want %v", keys, want)
	}
}

func TestAccountKVLatestIndexCanBeBufferedAndDiscarded(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x6c)
	buf := blockbuffer.New(sdb.db.DiskDB())
	var blockHash tcommon.Hash
	blockHash[0] = 0x01
	buf.BeginBlock(blockHash)
	sdb.SetAccountKVIndexStore(buf)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDelegation, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(sdb.db.DiskDB(), addr, 0, kvdomains.SystemDelegation, []byte("k")); err != nil || ok {
		t.Fatalf("latest index reached disk before buffer flush: ok=%v err=%v", ok, err)
	}
	if got, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDelegation, []byte("k")); err != nil || !ok || string(got) != "v" {
		t.Fatalf("buffer latest = %q ok=%v err=%v", got, ok, err)
	}
	buf.CommitBlock()
	buf.DiscardBlock(blockHash)
	if _, ok, err := rawdb.ReadStateKVLatest(buf, addr, 0, kvdomains.SystemDelegation, []byte("k")); err != nil || ok {
		t.Fatalf("discarded latest index visible: ok=%v err=%v", ok, err)
	}
}

func TestAccountKVLatestIndexReadThroughIsOptIn(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x6d)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDelegation, []byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemDelegation, []byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.SystemDelegation, []byte("k")); err != nil || !ok || string(got) != "v1" {
		t.Fatalf("root read = %q ok=%v err=%v, want v1", got, ok, err)
	}
	reopened.SetAccountKVIndexStore(sdb.db.DiskDB())
	reopened.SetAccountKVIndexReads(true)
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.SystemDelegation, []byte("k")); err != nil || !ok || string(got) != "v2" {
		t.Fatalf("latest-index read = %q ok=%v err=%v, want v2", got, ok, err)
	}
}

func TestAccountKVFinalWriteSkipsSnapshotJournal(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x6e)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	snap := sdb.Snapshot()
	if err := sdb.SetAccountKVFinal(addr, kvdomains.SystemDelegation, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	sdb.RevertToSnapshot(snap)
	if got, ok, err := sdb.GetAccountKV(addr, kvdomains.SystemDelegation, []byte("k")); err != nil || !ok || string(got) != "v" {
		t.Fatalf("final write after snapshot revert = %q ok=%v err=%v, want v", got, ok, err)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
