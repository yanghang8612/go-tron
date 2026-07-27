package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestGetOrCreateAccountMarksExportedWrapperEscaped(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xa1)
	obj := sdb.GetOrCreateAccount(addr)
	if obj == nil || !obj.wrapperEscaped {
		t.Fatal("exported state object wrapper was left eligible for identity reuse")
	}
}

func TestCreateAccountKeepsInternalWrapperPoolable(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xa2)
	if account := sdb.CreateAccount(addr, corepb.AccountType_Normal); account == nil {
		t.Fatal("CreateAccount returned nil")
	}
	obj := sdb.stateObjects[addr]
	if obj == nil || obj.wrapperEscaped {
		t.Fatal("internal account mutator unnecessarily escaped the state object wrapper")
	}
}

func TestStateObjectWorkingSetEvictsAccountsNotReusedForFourBlocks(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	a := testAddr(0xb1)
	b := testAddr(0xb2)
	sdb.CreateAccount(a, corepb.AccountType_Normal)
	sdb.CreateAccount(b, corepb.AccountType_Normal)
	sdb.AddBalance(a, 11)
	sdb.AddBalance(b, 22)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(sdb.stateObjects) != 2 {
		t.Fatalf("first block retained %d accounts, want 2", len(sdb.stateObjects))
	}

	// The next block reuses only a. The generational cache deliberately keeps b
	// across the short idle window so recurring access does not rehydrate it.
	if got := sdb.GetBalance(a); got != 11 {
		t.Fatalf("balance(a) = %d, want 11", got)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := sdb.stateObjects[a]; !ok {
		t.Fatal("account reused by the current block was evicted")
	}
	if _, ok := sdb.stateObjects[b]; !ok {
		t.Fatal("account from the preceding generation was evicted too early")
	}

	// A second complete block without b moves it into the oldest retained
	// generation, but does not expire it yet.
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := sdb.stateObjects[a]; !ok {
		t.Fatal("newer-generation account was evicted too early")
	}
	if _, ok := sdb.stateObjects[b]; !ok {
		t.Fatal("oldest retained account was evicted too early")
	}

	// A third complete block moves b into the ancient retained generation.
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := sdb.stateObjects[a]; !ok {
		t.Fatal("newer-generation account was evicted too early")
	}
	if _, ok := sdb.stateObjects[b]; !ok {
		t.Fatal("ancient retained account was evicted too early")
	}

	// After a fourth complete block without b, its generation expires. a was
	// touched one block later, so its duplicate in b's generation is skipped.
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := sdb.stateObjects[a]; !ok {
		t.Fatal("newer-generation account was evicted with the expired generation")
	}
	if _, ok := sdb.stateObjects[b]; ok {
		t.Fatal("account idle for four blocks was retained")
	}
	if got := sdb.GetBalance(b); got != 22 {
		t.Fatalf("reloaded balance(b) = %d, want 22", got)
	}
}

func TestStateObjectWorkingSetCountsOlderGenerationReuse(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xb3)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 33)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	// Leave the account untouched for one complete block, moving it into the
	// older retained generation without expiring it.
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	before := stateObjectCacheOlderReuseCounter.Snapshot().Count()
	if got := sdb.GetBalance(addr); got != 33 {
		t.Fatalf("balance = %d, want 33", got)
	}
	if got := stateObjectCacheOlderReuseCounter.Snapshot().Count() - before; got != 1 {
		t.Fatalf("older-generation reuse delta = %d, want 1", got)
	}
}

func TestStateObjectWorkingSetCountsOldestGenerationReuse(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xb4)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 44)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	// Two untouched blocks move the account from previous -> older -> oldest.
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	before := stateObjectCacheOldestReuseCounter.Snapshot().Count()
	if got := sdb.GetBalance(addr); got != 44 {
		t.Fatalf("balance = %d, want 44", got)
	}
	if got := stateObjectCacheOldestReuseCounter.Snapshot().Count() - before; got != 1 {
		t.Fatalf("oldest-generation reuse delta = %d, want 1", got)
	}
}

func TestStateObjectWorkingSetCountsAncientGenerationReuse(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xb5)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 55)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	// Three untouched blocks move the account through previous, older, oldest,
	// and into the final ancient retained generation.
	for range 3 {
		if _, err := sdb.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	before := stateObjectCacheAncientReuseCounter.Snapshot().Count()
	if got := sdb.GetBalance(addr); got != 55 {
		t.Fatalf("balance = %d, want 55", got)
	}
	if got := stateObjectCacheAncientReuseCounter.Snapshot().Count() - before; got != 1 {
		t.Fatalf("ancient-generation reuse delta = %d, want 1", got)
	}
}

func TestRotateStateObjectWorkingSetClearsEvictedLastLookup(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addrs := []tcommon.Address{testAddr(0xc1), testAddr(0xc2), testAddr(0xc3)}
	for _, addr := range addrs {
		sdb.CreateAccount(addr, corepb.AccountType_Normal)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	// Mark only the middle account as part of the next block, then point the
	// one-entry fast cache at a cold account to exercise stale-pointer cleanup.
	if sdb.getStateObject(addrs[1]) == nil {
		t.Fatal("middle account missing")
	}
	evicted := sdb.stateObjects[addrs[0]]
	evicted.cacheStorageSlot(tcommon.Hash{1}, storageSlot{exists: true})
	cacheAccountFrozenBandwidthCanonicalOwned(evicted, &corepb.Account_Frozen{FrozenBalance: 1})
	canonicalSlot := (*accountFrozenBandwidthCanonicalSlot)(evicted.account.Proto().Frozen[:2])
	sdb.lastStateObject = evicted
	sdb.rotateStateObjectWorkingSet()
	// The first rotation moves the original working set into the older slot; a
	// second moves it into the oldest slot; a third into the ancient slot; and a
	// fourth expires accounts not touched in any intervening block.
	sdb.rotateStateObjectWorkingSet()
	sdb.rotateStateObjectWorkingSet()
	sdb.rotateStateObjectWorkingSet()

	if len(sdb.stateObjects) != 1 || sdb.stateObjects[addrs[1]] == nil {
		t.Fatalf("retained working set = %v, want only middle account", sdb.stateObjects)
	}
	if sdb.lastStateObject != nil {
		t.Fatal("lastStateObject retained an evicted account")
	}
	if canonicalSlot[0] != nil || evicted.accountFrozenBandwidthCanonicalPooled {
		t.Fatal("evicted account retained canonical frozen-bandwidth storage")
	}
	if evicted.storage != nil || evicted.storageHighWater != 0 {
		t.Fatal("evicted account retained its private storage cache")
	}
	if len(sdb.retainedStateObjects) != 0 || len(sdb.olderRetainedStateObjects) != 0 || len(sdb.oldestRetainedStateObjects) != 0 || len(sdb.ancientRetainedStateObjects) != 1 || len(sdb.touchedStateObjects) != 0 {
		t.Fatalf("rotated slices = retained %d older %d oldest %d ancient %d touched %d, want 0/0/0/1/0",
			len(sdb.retainedStateObjects), len(sdb.olderRetainedStateObjects), len(sdb.oldestRetainedStateObjects), len(sdb.ancientRetainedStateObjects), len(sdb.touchedStateObjects))
	}
	if sdb.stateObjects[addrs[1]].cacheTouched {
		t.Fatal("retained account remained marked as touched after rotation")
	}

	// An empty following block drops the final retained account after its fourth
	// idle generation.
	sdb.rotateStateObjectWorkingSet()
	if len(sdb.stateObjects) != 0 {
		t.Fatalf("empty block retained %d accounts, want 0", len(sdb.stateObjects))
	}
}

func TestRotateStateObjectWorkingSetBoundsHotContractStorage(t *testing.T) {
	sdb, err := New(tcommon.Hash{}, NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0xd1)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	obj := sdb.stateObjects[addr]
	obj.dirty = false
	obj.accountDirty = false
	obj.created = false
	obj.ensureStorage()
	for i := 0; i <= maxStateObjectCachedStorageSlots; i++ {
		obj.storage[tcommon.Hash{byte(i >> 8), byte(i)}] = storageSlot{exists: true}
	}

	sdb.rotateStateObjectWorkingSet()
	if obj.storage != nil {
		t.Fatalf("oversized storage cache retained %d slots", len(obj.storage))
	}
	if sdb.stateObjects[addr] != obj {
		t.Fatal("bounding storage slots evicted the live account object")
	}
}
