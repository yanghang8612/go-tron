package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const (
	testForkVersion = int32(27)
	testVoteUpgrade = byte(0x01)
)

func TestRootedStoreLegacyFlatWritesAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)
	db := NewRootedStore(sdb, rawdb.NewMemoryDatabase())
	witness := testAddr(0x31)
	contract := testAddr(0x32)
	owner := testAddr(0x33)
	receiver := testAddr(0x34)

	w := types.NewWitness(witness, "url")
	w.SetVoteCount(7)
	rawdb.WriteWitness(db, witness, w)
	rawdb.WriteForkStats(db, testForkVersion, []byte{testVoteUpgrade})
	if err := rawdb.WriteDelegatedResourceV2(db, owner, receiver, false, &rawdb.DelegatedResource{
		From: owner, To: receiver, FrozenBalanceForEnergy: 123,
	}); err != nil {
		t.Fatalf("write delegation: %v", err)
	}
	cs := types.NewContractState(9)
	cs.AddEnergyUsage(456)
	if err := rawdb.WriteContractState(db, contract, cs); err != nil {
		t.Fatalf("write contract state: %v", err)
	}
	if err := rawdb.WriteZKProofResult(db, []byte("tx-proof"), true); err != nil {
		t.Fatalf("write zk proof: %v", err)
	}

	root1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit root1: %v", err)
	}
	atRoot1, err := New(root1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	rootedAt1 := NewRootedStore(atRoot1, nil)
	if got := rawdb.ReadWitness(rootedAt1, witness); got == nil || got.VoteCount() != 7 {
		t.Fatalf("rooted witness = %#v, want vote count 7", got)
	}
	if got := rawdb.ReadForkStats(rootedAt1, testForkVersion); len(got) != 1 || got[0] != testVoteUpgrade {
		t.Fatalf("rooted fork stats = %v", got)
	}
	if got := rawdb.ReadDelegatedResourceV2(rootedAt1, owner, receiver, false); got == nil || got.FrozenBalanceForEnergy != 123 {
		t.Fatalf("rooted delegation = %#v", got)
	}
	if got := rawdb.ReadContractState(rootedAt1, contract); got == nil || got.EnergyUsage() != 456 {
		t.Fatalf("rooted contract state = %#v", got)
	}
	if ok, exists := rawdb.ReadZKProofResult(rootedAt1, []byte("tx-proof")); !exists || !ok {
		t.Fatalf("rooted zk proof = ok:%v exists:%v", ok, exists)
	}

	rawdb.WriteWitness(db, witness, types.NewWitness(witness, "new-url"))
	root2, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit root2: %v", err)
	}
	if root1 == root2 {
		t.Fatal("root did not move after rooted flat-store mutation")
	}
	atRoot2, err := New(root2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := rawdb.ReadWitness(NewRootedStore(atRoot1, nil), witness); got == nil || got.URL() != "new-url" {
		t.Fatalf("root1-open latest witness URL = %#v, want new-url", got)
	}
	if got := rawdb.ReadWitness(NewRootedStore(atRoot2, nil), witness); got == nil || got.URL() != "new-url" {
		t.Fatalf("root2 witness URL = %#v, want new-url", got)
	}
}

func TestRootedStoreReadsContractNativeKV(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x44)
	slot := tcommon.BytesToHash([]byte{0x01})
	value := tcommon.BytesToHash([]byte{0x99})

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, []byte{0xde, 0xad})
	sdb.SetState(addr, slot, value)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	rooted := NewRootedStore(reopened, nil)
	if got := rawdb.ReadCode(rooted, addr); string(got) != string([]byte{0xde, 0xad}) {
		t.Fatalf("rooted rawdb code = %x", got)
	}
	if got := rawdb.ReadStorage(rooted, addr, reopened.storageRowKey(addr, slot)); tcommon.BytesToHash(got) != value {
		t.Fatalf("rooted rawdb storage = %x, want %x", got, value)
	}
}

func TestRootedStoreMappedKeysDoNotFallbackToFlatMirror(t *testing.T) {
	sdb := newTestStateDB(t)
	witness := testAddr(0x55)
	contract := testAddr(0x56)
	slot := tcommon.BytesToHash([]byte{0x01})
	value := tcommon.BytesToHash([]byte{0x99})

	sdb.CreateAccount(contract, corepb.AccountType_Contract)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	atRoot, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	fallback := rawdb.NewMemoryDatabase()
	rawdb.WriteWitness(fallback, witness, types.NewWitness(witness, "future-url"))
	rawdb.WriteCode(fallback, contract, []byte{0xfe})
	rawdb.WriteStorage(fallback, contract, javaStorageRowKey(contract, slot, nil), value.Bytes())

	rooted := NewRootedStore(atRoot, fallback)
	if got := rawdb.ReadWitness(rooted, witness); got != nil {
		t.Fatalf("rooted witness fell back to flat mirror: %#v", got)
	}
	if got := rawdb.ReadCode(rooted, contract); len(got) != 0 {
		t.Fatalf("rooted code fell back to flat mirror: %x", got)
	}
	if got := rawdb.ReadStorage(rooted, contract, javaStorageRowKey(contract, slot, nil)); len(got) != 0 {
		t.Fatalf("rooted storage fell back to flat mirror: %x", got)
	}

	if err := fallback.Put([]byte("unmapped"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	got, err := rooted.Get([]byte("unmapped"))
	if err != nil || string(got) != "ok" {
		t.Fatalf("unmapped key should still fall through to fallback: got %q err %v", got, err)
	}
}
