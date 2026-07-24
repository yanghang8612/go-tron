package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestStateObjectWorkingSetEvictsAccountsNotReusedByNextBlock(t *testing.T) {
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

	// The next block reuses only a. A successful no-op commit must evict b but
	// retain a, bounding the cache to the immediately preceding block's working
	// set without changing the durable state that b reloads from.
	if got := sdb.GetBalance(a); got != 11 {
		t.Fatalf("balance(a) = %d, want 11", got)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := sdb.stateObjects[a]; !ok {
		t.Fatal("account reused by the current block was evicted")
	}
	if _, ok := sdb.stateObjects[b]; ok {
		t.Fatal("account not reused by the current block was retained")
	}
	if got := sdb.GetBalance(b); got != 22 {
		t.Fatalf("reloaded balance(b) = %d, want 22", got)
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
	sdb.lastStateObject = evicted
	sdb.rotateStateObjectWorkingSet()

	if len(sdb.stateObjects) != 1 || sdb.stateObjects[addrs[1]] == nil {
		t.Fatalf("retained working set = %v, want only middle account", sdb.stateObjects)
	}
	if sdb.lastStateObject != nil {
		t.Fatal("lastStateObject retained an evicted account")
	}
	if len(sdb.retainedStateObjects) != 1 || len(sdb.touchedStateObjects) != 0 {
		t.Fatalf("rotated slices = retained %d touched %d, want 1/0",
			len(sdb.retainedStateObjects), len(sdb.touchedStateObjects))
	}
	if sdb.stateObjects[addrs[1]].cacheTouched {
		t.Fatal("retained account remained marked as touched after rotation")
	}

	// An empty following block drops the final retained account.
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
