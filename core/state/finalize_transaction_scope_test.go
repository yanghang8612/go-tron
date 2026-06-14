package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// FinalizeTransaction must flip a written-to-zero storage row to non-existent at
// the transaction boundary (java-tron deletes a StorageRow whose value becomes
// zero). Before finalize the present-zero row still reads exists=true, which the
// SSTORE energy accounting relies on.
func TestFinalizeTransactionMarksWrittenZeroRowNonExistent(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	key := tcommon.Hash{0x01}

	sdb.SetState(addr, key, tcommon.Hash{0x42})
	sdb.SetState(addr, key, tcommon.Hash{}) // overwrite with zero

	if _, exists := sdb.GetStateWithExist(addr, key); !exists {
		t.Fatal("present-zero row should read exists=true before finalize")
	}

	sdb.FinalizeTransaction()

	if v, exists := sdb.GetStateWithExist(addr, key); exists || v != (tcommon.Hash{}) {
		t.Fatalf("after finalize want (0,false), got (%x,%v)", v, exists)
	}
}

// The zero-row marking must hold even when the object that was written to zero is
// NOT the object touched by a later transaction. This pins the cross-transaction
// behavior the per-transaction scoping must preserve.
func TestFinalizeTransactionZeroMarkPersistsAcrossUntouchedTx(t *testing.T) {
	sdb := newTestStateDB(t)
	a, b := testAddr(1), testAddr(2)
	key := tcommon.Hash{0x01}

	// tx0: write zero to A, finalize.
	sdb.SetState(a, key, tcommon.Hash{0x42})
	sdb.SetState(a, key, tcommon.Hash{})
	sdb.FinalizeTransaction()
	if _, exists := sdb.GetStateWithExist(a, key); exists {
		t.Fatal("A zero row should be non-existent after tx0 finalize")
	}

	// tx1: touch only B, finalize. A must stay non-existent.
	sdb.SetState(b, tcommon.Hash{0x09}, tcommon.Hash{0x07})
	sdb.FinalizeTransaction()
	if _, exists := sdb.GetStateWithExist(a, key); exists {
		t.Fatal("A zero row must remain non-existent after an unrelated tx")
	}
	if v := sdb.GetState(b, tcommon.Hash{0x09}); v != (tcommon.Hash{0x07}) {
		t.Fatalf("B slot corrupted: got %x", v)
	}
}

// Multiple distinct contracts each written to zero in the same transaction must
// all be marked at the single FinalizeTransaction call (the outer scoping must
// cover every touched object, not just one).
func TestFinalizeTransactionMarksAllTouchedContracts(t *testing.T) {
	sdb := newTestStateDB(t)
	key := tcommon.Hash{0x01}
	addrs := []tcommon.Address{testAddr(1), testAddr(2), testAddr(3)}
	for _, a := range addrs {
		sdb.SetState(a, key, tcommon.Hash{0x42})
		sdb.SetState(a, key, tcommon.Hash{})
	}
	sdb.FinalizeTransaction()
	for _, a := range addrs {
		if _, exists := sdb.GetStateWithExist(a, key); exists {
			t.Fatalf("contract %x zero row should be non-existent after finalize", a[19])
		}
	}
}

// Adversarial: an account created, written-to-zero and self-destructed inside a
// transaction, then reverted and recreated fresh, must NOT be deleted by a stale
// finalize candidate. This discriminates address-scoped finalize (correct) from a
// naive pointer-scoped finalize (which would carry the dead object and delete the
// recreated one via DeleteAccount's address lookup).
func TestFinalizeTransactionRecreatedAfterRevertNotDeleted(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(7)

	snap := sdb.Snapshot()
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetState(addr, tcommon.Hash{0x01}, tcommon.Hash{}) // zero write -> finalize candidate
	sdb.SelfDestruct(addr)                                 // self-destruct -> finalize candidate
	sdb.RevertToSnapshot(snap)                             // undo all; addr leaves stateObjects

	// Recreate the same address as a fresh, live contract.
	sdb.CreateAccount(addr, corepb.AccountType_Contract)

	sdb.FinalizeTransaction()

	if !sdb.AccountExists(addr) {
		t.Fatal("recreated account must survive finalize (stale candidate must not delete it)")
	}
	if sdb.HasSelfDestructed(addr) {
		t.Fatal("recreated account must not be self-destructed")
	}
}

// A self-destruct followed by FinalizeTransaction must delete the account for the
// next transaction (the outer scoping must still cover self-destructed objects).
func TestFinalizeTransactionScopedSelfDestructStillDeletes(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(9)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if !sdb.AccountExists(addr) {
		t.Fatal("account should exist before self-destruct")
	}
	sdb.SelfDestruct(addr)
	sdb.FinalizeTransaction()
	if sdb.AccountExists(addr) {
		t.Fatal("self-destructed account should be absent after finalize")
	}
}

// A row written to zero and committed must stay non-existent in a LATER block
// that only touches a different slot of the same contract. This pins the
// cross-commit persistence the inner-loop scoping relies on: storageExists is
// not reset at commit, so a later FinalizeTransaction need not re-scan the
// already-marked zero row — it must already read as absent.
func TestFinalizeTransactionZeroRowStaysNonExistentAcrossCommit(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(5)
	k := tcommon.Hash{0x01}
	k2 := tcommon.Hash{0x02}

	// Block N: write k non-zero then zero, finalize, commit.
	sdb.SetState(addr, k, tcommon.Hash{0x42})
	sdb.SetState(addr, k, tcommon.Hash{})
	sdb.FinalizeTransaction()
	if _, exists := sdb.GetStateWithExist(addr, k); exists {
		t.Fatal("k should be non-existent after block N finalize")
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	// Block N+1 on the same reused StateDB: write a DIFFERENT slot. k is not in
	// this block's dirtyStorage, yet must remain non-existent.
	sdb.SetState(addr, k2, tcommon.Hash{0x07})
	sdb.FinalizeTransaction()
	if _, exists := sdb.GetStateWithExist(addr, k); exists {
		t.Fatal("k must remain non-existent in a later block (inner-loop scoping must not lose it)")
	}
	if v, exists := sdb.GetStateWithExist(addr, k2); !exists || v != (tcommon.Hash{0x07}) {
		t.Fatalf("k2 = (%x,%v), want (07,true)", v, exists)
	}
}
