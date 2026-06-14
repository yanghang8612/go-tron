package state

import (
	"sync"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// dirtyAccountCommitPlans iterates s.dirtyObjects (an incrementally-maintained
// set of marked-dirty addresses) instead of scanning the whole stateObjects map.
// These tests pin the set's completeness (every dirty object is recorded), its
// per-block lifecycle (cleared at commit), and that commit output is unchanged
// across the adversarial paths (revert, recreate-after-selfdestruct, reuse, and
// async/deferred-fold commit).

func dirtyObjectsHas(s *StateDB, addr tcommon.Address) bool {
	_, ok := s.dirtyObjects[addr]
	return ok
}

func reopenStateDB(t *testing.T, root tcommon.Hash, db *Database) *StateDB {
	t.Helper()
	s, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// commitClearedStateDB returns a StateDB with addr created and committed, so the
// dirty set starts empty and the object is cached clean — the precondition for
// isolating a single later mutator's recording behavior.
func commitClearedStateDB(t *testing.T, addr tcommon.Address, accType corepb.AccountType) *StateDB {
	t.Helper()
	sdb := newTestStateDB(t)
	sdb.CreateAccount(addr, accType)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(sdb.dirtyObjects) != 0 {
		t.Fatalf("precondition: dirty set must be empty after commit, got %d", len(sdb.dirtyObjects))
	}
	return sdb
}

// --- population: each dirtiness category records its address ---

// markDirty hook + getStateObject back-pointer: a mutation on a pre-existing,
// clean (cached) account records it.
func TestDirtyObjects_AccountMutationRecorded(t *testing.T) {
	addr := testAddr(1)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 100)
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("AddBalance must record the address in dirtyObjects")
	}
}

// getStateObject LOAD-path back-pointer: a mutation on an account that is only on
// disk (fresh StateDB, empty cache) records it after the load.
func TestDirtyObjects_MutationAfterDiskLoadRecorded(t *testing.T) {
	addr := testAddr(2)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Normal)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened := reopenStateDB(t, root, sdb.db)
	reopened.AddBalance(addr, 7) // getStateObject load path -> markDirty
	if !dirtyObjectsHas(reopened, addr) {
		t.Fatal("AddBalance after disk load must record the address")
	}
}

// getStateObject early-return back-pointer: an account hydrated via
// LoadAccountReference for an address NOT yet on disk takes the direct-insert
// path (newStateObject without a back-pointer). Its first mutation must still
// record it, which only the getStateObject early-return heal provides.
func TestDirtyObjects_MutationAfterLoadReferenceRecorded(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(3)
	acc := types.NewAccount(addr, corepb.AccountType_Normal) // not on disk
	sdb.LoadAccountReference(acc)                            // inserts a cached object with no back-pointer
	sdb.AddBalance(addr, 9)                                  // getStateObject early-return must heal the back-pointer
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("AddBalance after LoadAccountReference must record the address")
	}
}

// GetOrCreateAccount hook: creating a brand-new account records it even with no
// subsequent markDirty-based mutation (the object is born dirty).
func TestDirtyObjects_NewAccountRecordedOnCreate(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(4)
	sdb.GetOrCreateAccount(addr)
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("GetOrCreateAccount must record the born-dirty account")
	}
}

func TestDirtyObjects_StorageWriteRecorded(t *testing.T) {
	addr := testAddr(5)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	sdb.SetState(addr, tcommon.Hash{0x01}, tcommon.Hash{0x02})
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("SetState must record the address")
	}
}

func TestDirtyObjects_CodeWriteRecorded(t *testing.T) {
	addr := testAddr(6)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, []byte{0x60, 0x00, 0xf3})
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("SetCode must record the address")
	}
}

func TestDirtyObjects_ContractMetaRecorded(t *testing.T) {
	addr := testAddr(7)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{Name: "meta"})
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("SetContract must record the address")
	}
}

func TestDirtyObjects_KVWriteRecorded(t *testing.T) {
	addr := testAddr(8)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	if err := sdb.SetAccountKV(addr, kvdomains.ContractStorage, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("SetAccountKV must record the address")
	}
}

func TestDirtyObjects_GenerationResetRecorded(t *testing.T) {
	addr := testAddr(9)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	if err := sdb.ResetAccountKV(addr); err != nil {
		t.Fatal(err)
	}
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("ResetAccountKV must record the address")
	}
}

func TestDirtyObjects_SelfDestructRecorded(t *testing.T) {
	addr := testAddr(10)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	sdb.SelfDestruct(addr)
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("SelfDestruct must record the address")
	}
}

func TestDirtyObjects_DeleteAccountRecorded(t *testing.T) {
	addr := testAddr(11)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Contract)
	sdb.DeleteAccount(addr)
	if !dirtyObjectsHas(sdb, addr) {
		t.Fatal("DeleteAccount must record the address")
	}
}

// A pure read must NOT add the account to the dirty set — that is the whole point
// of the optimization (read-only cached accounts are skipped at commit).
func TestDirtyObjects_ReadDoesNotRecord(t *testing.T) {
	addr := testAddr(12)
	sdb := commitClearedStateDB(t, addr, corepb.AccountType_Normal)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened := reopenStateDB(t, root, sdb.db)
	_ = reopened.GetBalance(addr) // read -> loads object, but must not mark dirty
	if dirtyObjectsHas(reopened, addr) {
		t.Fatal("a read must not record the address in dirtyObjects")
	}
}

// --- lifecycle ---

func TestDirtyObjects_ClearedAfterCommit(t *testing.T) {
	sdb := newTestStateDB(t)
	a, b := testAddr(1), testAddr(2)
	sdb.CreateAccount(a, corepb.AccountType_Normal)
	sdb.CreateAccount(b, corepb.AccountType_Normal)
	sdb.AddBalance(a, 100)
	if len(sdb.dirtyObjects) == 0 {
		t.Fatal("dirty set must be populated before commit")
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(sdb.dirtyObjects) != 0 {
		t.Fatalf("dirty set must be empty after commit, got %d", len(sdb.dirtyObjects))
	}
}

// Core safety invariant: every dirty stateObject is present in the set (the set
// is a superset of {addr : stateObjects[addr].dirty}); the !dirty guard handles
// the rest.
func TestDirtyObjects_SupersetOfDirtyFlag(t *testing.T) {
	sdb := newTestStateDB(t)
	// A few committed (clean) accounts.
	for i := byte(1); i <= 3; i++ {
		sdb.CreateAccount(testAddr(i), corepb.AccountType_Normal)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	// Mix of reads (clean) and mutations of several kinds.
	_ = sdb.GetBalance(testAddr(1)) // clean read
	sdb.AddBalance(testAddr(2), 5)  // account dirty
	sdb.CreateAccount(testAddr(4), corepb.AccountType_Contract)
	sdb.SetState(testAddr(4), tcommon.Hash{0x01}, tcommon.Hash{0x02}) // storage dirty
	sdb.SetCode(testAddr(4), []byte{0x60})                            // code dirty
	snap := sdb.Snapshot()
	sdb.AddBalance(testAddr(3), 1)
	sdb.RevertToSnapshot(snap) // reverted (re-dirtied) account

	for addr, obj := range sdb.stateObjects {
		if obj.dirty {
			if !dirtyObjectsHas(sdb, addr) {
				t.Fatalf("dirty object %x is missing from dirtyObjects", addr)
			}
		}
	}
}

// --- reuse across blocks (maintenance-crossing analog at StateDB level) ---

func TestDirtyObjects_ReusedAcrossBlocksTracksPerBlock(t *testing.T) {
	sdb := newTestStateDB(t)
	a, b := testAddr(1), testAddr(2)

	// block 1.
	sdb.CreateAccount(a, corepb.AccountType_Normal)
	sdb.AddBalance(a, 10)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(sdb.dirtyObjects) != 0 {
		t.Fatal("dirty set must be cleared after block 1")
	}

	// block 2 on the SAME reused StateDB: read a (clean), mutate b.
	_ = sdb.GetBalance(a)
	sdb.CreateAccount(b, corepb.AccountType_Normal)
	sdb.AddBalance(b, 20)
	if dirtyObjectsHas(sdb, a) {
		t.Fatal("read-only account a must not be in block 2's dirty set")
	}
	if !dirtyObjectsHas(sdb, b) {
		t.Fatal("mutated account b must be in block 2's dirty set")
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	final := reopenStateDB(t, root, sdb.db)
	if got := final.GetBalance(a); got != 10 {
		t.Fatalf("a balance: want 10, got %d", got)
	}
	if got := final.GetBalance(b); got != 20 {
		t.Fatalf("b balance: want 20, got %d", got)
	}
}

// --- equivalence: every dirty account is committed; read-only ones are not needed ---

func TestDirtyObjects_CommitPersistsAllDirtyAccounts(t *testing.T) {
	sdb := newTestStateDB(t)
	const n = 16
	for i := byte(1); i <= n; i++ {
		sdb.CreateAccount(testAddr(i), corepb.AccountType_Normal)
		sdb.AddBalance(testAddr(i), int64(i)*1000)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen, READ every account (populating the cache with clean read-only
	// objects), then mutate only the even ones and commit.
	reopened := reopenStateDB(t, root, sdb.db)
	for i := byte(1); i <= n; i++ {
		_ = reopened.GetBalance(testAddr(i)) // clean read
	}
	for i := byte(2); i <= n; i += 2 {
		reopened.AddBalance(testAddr(i), 1) // even accounts dirtied
	}
	root2, err := reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}

	final := reopenStateDB(t, root2, sdb.db)
	for i := byte(1); i <= n; i++ {
		want := int64(i) * 1000
		if i%2 == 0 {
			want++
		}
		if got := final.GetBalance(testAddr(i)); got != want {
			t.Fatalf("account %d balance: want %d, got %d", i, want, got)
		}
	}
}

// --- adversarial: revert ---

// A created-then-reverted account leaves a stale entry in the set (over-approx),
// but it is removed from stateObjects, so commit must skip it via the nil guard.
func TestDirtyObjects_CreatedThenRevertedSkippedAtCommit(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(5)
	snap := sdb.Snapshot()
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 999)
	sdb.RevertToSnapshot(snap)

	if _, ok := sdb.stateObjects[addr]; ok {
		t.Fatal("created-then-reverted account should be removed from stateObjects")
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit must tolerate a stale dirty-set entry: %v", err)
	}
	if sdb.AccountExists(addr) {
		t.Fatal("reverted-create account must not exist after commit")
	}
}

// A reverted mutation on a pre-existing account re-dirties it (over-approx,
// matching the dirtyWitnesses idiom); commit writes back the original value.
func TestDirtyObjects_RevertedMutationCommitsOriginalValue(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(6)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 100)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	reopened := reopenStateDB(t, root, sdb.db)
	snap := reopened.Snapshot()
	reopened.AddBalance(addr, 555)
	reopened.RevertToSnapshot(snap)
	root2, err := reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}

	final := reopenStateDB(t, root2, sdb.db)
	if got := final.GetBalance(addr); got != 100 {
		t.Fatalf("balance after reverted mutation: want 100, got %d", got)
	}
}

// --- adversarial: recreate after self-destruct ---

func TestDirtyObjects_RecreateAfterSelfDestructCommits(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(7)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.AddBalance(addr, 1)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Next block: self-destruct + finalize (promotes to deleted), then recreate
	// fresh in the same block and set a new balance.
	reopened := reopenStateDB(t, root, sdb.db)
	reopened.SelfDestruct(addr)
	reopened.FinalizeTransaction()
	reopened.CreateAccount(addr, corepb.AccountType_Normal)
	reopened.AddBalance(addr, 4242)
	root2, err := reopened.Commit()
	if err != nil {
		t.Fatal(err)
	}

	final := reopenStateDB(t, root2, sdb.db)
	if !final.AccountExists(addr) {
		t.Fatal("recreated account must exist after commit")
	}
	if got := final.GetBalance(addr); got != 4242 {
		t.Fatalf("recreated balance: want 4242, got %d", got)
	}
}

// --- adversarial: async (deferred-fold) commit ---

// The deferred-fold (async) commit path must clear the dirty set and produce a
// byte-identical commitment root to the synchronous path.
func TestDirtyObjects_DeferFoldMatchesSyncRoot(t *testing.T) {
	mutate := func(s *StateDB) {
		s.CreateAccount(testAddr(1), corepb.AccountType_Normal)
		s.AddBalance(testAddr(1), 500)
		s.CreateAccount(testAddr(2), corepb.AccountType_Contract)
		s.SetState(testAddr(2), tcommon.Hash{0x01}, tcommon.Hash{0x42})
		s.SetCode(testAddr(2), []byte{0x60, 0x00})
	}

	syncSDB := newTestStateDB(t)
	mutate(syncSDB)
	syncRoot, err := syncSDB.Commit()
	if err != nil {
		t.Fatal(err)
	}

	asyncSDB := newTestStateDB(t)
	mutate(asyncSDB)
	asyncSDB.SetDeferFold(true)
	zeroRoot, _, err := asyncSDB.CommitWithStats()
	if err != nil {
		t.Fatal(err)
	}
	if zeroRoot != (tcommon.Hash{}) {
		t.Fatalf("deferfold commit should return a zero root, got %x", zeroRoot)
	}
	if len(asyncSDB.dirtyObjects) != 0 {
		t.Fatalf("deferfold commit must clear the dirty set, got %d entries", len(asyncSDB.dirtyObjects))
	}
	captured := asyncSDB.TakeCapturedFold()
	if captured == nil {
		t.Fatal("expected a captured fold")
	}
	asyncRoot, err := captured.Fold(asyncSDB.accountKVIndex())
	if err != nil {
		t.Fatal(err)
	}
	if syncRoot != asyncRoot {
		t.Fatalf("async/sync root mismatch: sync=%x async=%x", syncRoot, asyncRoot)
	}
}

// The captured fold (run by the commit worker) must not touch the StateDB's
// dirty set or stateObjects, so running it concurrently with the next block's
// mutations is race-free. Run under -race.
func TestDirtyObjects_DeferFoldWorkerNoRaceWithExecMutation(t *testing.T) {
	sdb := newTestStateDB(t)
	// Pre-create the accounts the exec goroutine touches so its mutations hit the
	// cached objects (no disk/index reads) — isolating the test to the dirty-set
	// interaction between the fold worker and the exec thread.
	execA, execB := testAddr(20), testAddr(21)
	sdb.CreateAccount(execA, corepb.AccountType_Normal)
	sdb.CreateAccount(execB, corepb.AccountType_Normal)
	sdb.CreateAccount(testAddr(1), corepb.AccountType_Normal)
	sdb.AddBalance(testAddr(1), 100)
	sdb.SetDeferFold(true)
	if _, _, err := sdb.CommitWithStats(); err != nil {
		t.Fatal(err)
	}
	captured := sdb.TakeCapturedFold()
	if captured == nil {
		t.Fatal("expected a captured fold")
	}
	idx := sdb.accountKVIndex()

	var (
		wg      sync.WaitGroup
		foldErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, foldErr = captured.Fold(idx)
	}()
	go func() {
		defer wg.Done()
		sdb.AddBalance(execA, 1)
		sdb.AddBalance(execB, 2)
	}()
	wg.Wait()
	if foldErr != nil {
		t.Fatal(foldErr)
	}
	if !dirtyObjectsHas(sdb, execA) || !dirtyObjectsHas(sdb, execB) {
		t.Fatal("exec-thread mutations during a worker fold must record in the dirty set")
	}
}

// --- Copy ---

func TestDirtyObjects_CopySeedsDirtySet(t *testing.T) {
	sdb := newTestStateDB(t)
	a := testAddr(1)
	sdb.CreateAccount(a, corepb.AccountType_Normal)
	sdb.AddBalance(a, 100)

	cp, err := sdb.Copy()
	if err != nil {
		t.Fatal(err)
	}
	if !dirtyObjectsHas(cp, a) {
		t.Fatal("Copy must seed the dirty address into the copy's set")
	}

	// A mutation on the copy records into the copy's set, not the original's.
	b := testAddr(2)
	cp.CreateAccount(b, corepb.AccountType_Normal)
	if !dirtyObjectsHas(cp, b) {
		t.Fatal("mutation on the copy must record in the copy's set")
	}
	if dirtyObjectsHas(sdb, b) {
		t.Fatal("mutation on the copy must not leak into the original's set")
	}

	// The copy commits and persists the dirty account.
	root, err := cp.Commit()
	if err != nil {
		t.Fatal(err)
	}
	final := reopenStateDB(t, root, cp.db)
	if got := final.GetBalance(a); got != 100 {
		t.Fatalf("copy-committed balance: want 100, got %d", got)
	}
}
