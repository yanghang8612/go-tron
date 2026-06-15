package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func transientHash(v byte) tcommon.Hash {
	var h tcommon.Hash
	h[31] = v
	return h
}

// TestTransientStorageNamespacedAndJournaled locks the EIP-1153 (Cancun)
// transient-storage semantics ported to match java-tron's per-frame child
// RepositoryImpl (HashBasedTable<address,key>, committed to the parent only on
// success and discarded on revert): values are namespaced per (address, slot),
// a write is undone by RevertToSnapshot with its frame, and all transient
// storage is discarded at FinalizeTransaction.
func TestTransientStorageNamespacedAndJournaled(t *testing.T) {
	sdb := newTestStateDB(t)
	addrA := testAddr(1)
	addrB := testAddr(2)
	key := transientHash(7)
	val1 := transientHash(42)

	// Round-trip under A.
	sdb.SetTransientState(addrA, key, val1)
	if got := sdb.GetTransientState(addrA, key); got != val1 {
		t.Fatalf("A/slot7 = %x, want %x", got, val1)
	}
	// Address namespacing: B/slot7 and A/slot8 are independent of A/slot7.
	if got := sdb.GetTransientState(addrB, key); got != (tcommon.Hash{}) {
		t.Fatalf("B/slot7 = %x, want zero (namespaced by address)", got)
	}
	if got := sdb.GetTransientState(addrA, transientHash(8)); got != (tcommon.Hash{}) {
		t.Fatalf("A/slot8 = %x, want zero (namespaced by slot)", got)
	}

	// Frame revert restores the PRE-snapshot value (not zero).
	snap := sdb.Snapshot()
	val2 := transientHash(99)
	sdb.SetTransientState(addrA, key, val2)
	if got := sdb.GetTransientState(addrA, key); got != val2 {
		t.Fatalf("A/slot7 after overwrite = %x, want %x", got, val2)
	}
	sdb.RevertToSnapshot(snap)
	if got := sdb.GetTransientState(addrA, key); got != val1 {
		t.Fatalf("A/slot7 after revert = %x, want pre-snapshot %x", got, val1)
	}

	// Reverting a write to a previously-absent slot leaves it reading as zero.
	snap2 := sdb.Snapshot()
	sdb.SetTransientState(addrB, key, val2)
	sdb.RevertToSnapshot(snap2)
	if got := sdb.GetTransientState(addrB, key); got != (tcommon.Hash{}) {
		t.Fatalf("B/slot7 after revert-to-absent = %x, want zero", got)
	}

	// Per-transaction discard.
	sdb.FinalizeTransaction()
	if got := sdb.GetTransientState(addrA, key); got != (tcommon.Hash{}) {
		t.Fatalf("A/slot7 after FinalizeTransaction = %x, want zero (EIP-1153 discard)", got)
	}
}

// TestTransientStorageNoOpWriteSkipsJournal locks the go-ethereum optimization
// that a write which does not change the slot adds no journal entry — including
// a zero write to an already-absent slot.
func TestTransientStorageNoOpWriteSkipsJournal(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	key := transientHash(1)
	val := transientHash(5)

	sdb.SetTransientState(addr, key, val)
	n := sdb.journal.length()
	sdb.SetTransientState(addr, key, val) // identical value → no-op
	if got := sdb.journal.length(); got != n {
		t.Fatalf("no-op overwrite journaled: length %d -> %d", n, got)
	}

	m := sdb.journal.length()
	sdb.SetTransientState(addr, transientHash(2), tcommon.Hash{}) // zero into absent slot → no-op
	if got := sdb.journal.length(); got != m {
		t.Fatalf("zero-write to absent slot journaled: length %d -> %d", m, got)
	}
}
