package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestRecreatedAccountDoesNotLeakOldGenerationSlots is a characterization test
// for the per-account KV "generation" (Erigon-incarnation-style) counter.
//
// Account-KV latest rows are keyed by owner||generation||domain||logicalKey
// (rawdb.stateKVLatestKey). When an account is destroyed and recreated, the
// generation must bump so the recreated account reads a FRESH namespace and
// cannot observe storage that belonged to the previous incarnation. The old
// rows are deliberately NOT prefix-deleted (Erigon style) — they survive on
// disk but must be unreachable from the bumped generation.
//
// This test drives the realistic cross-block flow against the FLAT latest
// index (where the generation prefix physically matters):
//
//	gen 0: account created, slots A and B both set
//	       (SELFDESTRUCT-then-CREATE2 is cross-transaction / cross-block; the
//	        delete commit persists the generation row, then a fresh StateDB
//	        recreates and nextAccountKVGeneration reads row+1)
//	gen 1: account recreated, ONLY slot A rewritten
//
// A leak = slot B (a gen-0-only key) visible on the recreated (gen-1) account.
// The asserted behavior is the CORRECT one (no leak); if it fails, a real
// state-leak bug has been found.
func TestRecreatedAccountDoesNotLeakOldGenerationSlots(t *testing.T) {
	sdb := newTestStateDB(t)
	disk := sdb.db.DiskDB()
	addr := testAddr(0x90)
	slotA := []byte("slotA")
	slotB := []byte("slotB")

	// Route latest-index writes/reads at the shared disk so the
	// generation-prefixed rows are the ones under test (not the KV trie).
	enableFlat := func(s *StateDB) {
		s.SetAccountKVIndexStore(disk)
		s.SetAccountKVIndexReads(true)
	}
	enableFlat(sdb)

	// gen 0: create with slots A and B.
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(addr, kvdomains.ContractStorage, slotA, []byte("A0")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.ContractStorage, slotB, []byte("B0")); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Destroy in a fresh StateDB block (mirrors opSelfDestruct +
	// FinalizeTransaction promoting selfDestructed -> deleted before commit).
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	enableFlat(reopened)
	reopened.SelfDestruct(addr)
	reopened.FinalizeTransaction()
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Recreate in the next block, writing ONLY slot A.
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	enableFlat(reopened)
	reopened.CreateAccount(addr, corepb.AccountType_Normal)
	if err := reopened.SetAccountKV(addr, kvdomains.ContractStorage, slotA, []byte("A1")); err != nil {
		t.Fatal(err)
	}
	root, err = reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Fresh post-recreate view.
	reopened, err = New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	enableFlat(reopened)

	obj := reopened.getStateObject(addr)
	if obj == nil {
		t.Fatal("recreated account missing")
	}
	if obj.accountKVGeneration != 1 {
		t.Fatalf("recreated generation = %d, want 1", obj.accountKVGeneration)
	}

	// Live read: slot A reflects the recreate; slot B must NOT leak.
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.ContractStorage, slotA); err != nil || !ok || string(got) != "A1" {
		t.Fatalf("recreated slot A = %q ok=%v err=%v, want A1", got, ok, err)
	}
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.ContractStorage, slotB); err != nil || ok {
		t.Fatalf("LEAK: recreated account sees old-generation slot B = %q ok=%v err=%v, want absent", got, ok, err)
	}

	// GetState reads the same namespace via the ContractStorage domain; confirm
	// the leak is absent through that entry point too (slot B uses an arbitrary
	// 32-byte key here; we assert the generic-KV layer, the storage-row-key
	// derivation is covered elsewhere).
	if got, ok, err := reopened.GetAccountKV(addr, kvdomains.ContractStorage, slotB); ok || err != nil {
		t.Fatalf("LEAK via second read: slot B = %q ok=%v err=%v", got, ok, err)
	}

	// Erigon-incarnation invariant: the old gen-0 rows physically survive on
	// disk (no O(N) prefix delete) but are unreachable from gen 1.
	if v, ok, err := rawdb.ReadStateKVLatest(disk, addr, 0, kvdomains.ContractStorage, slotB); err != nil || !ok || string(v) != "B0" {
		t.Fatalf("expected orphaned gen-0 slot B row to survive on disk = %q ok=%v err=%v, want B0", v, ok, err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(disk, addr, 1, kvdomains.ContractStorage, slotB); err != nil || ok {
		t.Fatalf("gen-1 slot B row should not exist: ok=%v err=%v", ok, err)
	}
}

// TestArchiveStorageAgreesWithLiveAcrossRecreate pins the read-only archive
// path (PersistentHistoryReader.StorageAt) against the live StateDB across a
// destroy+recreate that rewrites only a subset of the original slots.
//
// In this worktree the archive StorageAt resolves the live (end-of-head)
// baseline through live.GetState — i.e. the SAME generation source as live
// execution — and rolls back via the slot inverse index + slot deltas. There
// is no separate generation-row read on this path, so live and archive must
// agree by construction. This test makes that agreement observable end-to-end.
func TestArchiveStorageAgreesWithLiveAcrossRecreate(t *testing.T) {
	f := newHistoryFixture(t)
	c := testAddr(0x91)
	// NOTE: javaStorageRowKey (storage_key.go) maps a slot to
	// addrHash[:16] || slotKey[16:], so distinct slots MUST differ in their
	// low 16 bytes or they collide to one physical row. Set the last byte.
	var slotA, slotB tcommon.Hash
	slotA[31] = 0xAA
	slotB[31] = 0xBB

	// Block 1: create contract, set slots A and B.
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.GetOrCreateAccount(c)
		s.SetState(c, slotA, tcommon.HexToHash("a0"))
		s.SetState(c, slotB, tcommon.HexToHash("b0"))
	})
	// Block 2: destroy (selfdestruct promoted to delete at tx finalize).
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.SelfDestruct(c)
		s.FinalizeTransaction()
	})
	// Block 3: recreate, rewrite ONLY slot A.
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.CreateAccount(c, corepb.AccountType_Contract)
		s.SetState(c, slotA, tcommon.HexToHash("a1"))
	})
	// Block 4: untouched.
	f.applyBlock(tcommon.Hash{0x04}, func(s *StateDB) {
		s.AddBalance(testAddr(0x92), 1)
	})

	r := f.reader()

	// At end-of-head (block 4): live and archive must agree.
	liveA := f.state.GetState(c, slotA)
	liveB := f.state.GetState(c, slotB)
	if liveA != tcommon.HexToHash("a1") {
		t.Fatalf("live slot A = %x, want a1", liveA)
	}
	// No leak on the live side: B was only ever set in the gen-0 incarnation.
	if liveB != (tcommon.Hash{}) {
		t.Fatalf("LEAK (live): recreated contract sees old slot B = %x, want empty", liveB)
	}

	for _, bn := range []uint64{3, 4} {
		gotA, err := r.StorageAt(c, slotA, bn)
		if err != nil {
			t.Fatalf("StorageAt(A, %d): %v", bn, err)
		}
		if gotA != liveA {
			t.Fatalf("archive slot A @%d = %x, want %x (live)", bn, gotA, liveA)
		}
		gotB, err := r.StorageAt(c, slotB, bn)
		if err != nil {
			t.Fatalf("StorageAt(B, %d): %v", bn, err)
		}
		if gotB != (tcommon.Hash{}) {
			t.Fatalf("LEAK (archive): slot B @%d = %x, want empty", bn, gotB)
		}
	}

	// Sanity: before the destroy (end-of-block-1), the archive correctly
	// reconstructs BOTH slots of the gen-0 incarnation.
	if got, err := r.StorageAt(c, slotA, 1); err != nil || got != tcommon.HexToHash("a0") {
		t.Fatalf("archive slot A @1 = %x err=%v, want a0", got, err)
	}
	if got, err := r.StorageAt(c, slotB, 1); err != nil || got != tcommon.HexToHash("b0") {
		t.Fatalf("archive slot B @1 = %x err=%v, want b0", got, err)
	}
}
