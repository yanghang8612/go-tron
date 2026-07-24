package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestNewStateObjectDefaultsKVFields(t *testing.T) {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	obj := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Normal))
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
	if obj.storage != nil || obj.kvDirty != nil {
		t.Fatal("new state object eagerly allocated storage maps")
	}
	obj.stageKV(kvdomains.ContractMetadata, []byte("key"), []byte("value"))
	if obj.kvDirty == nil || len(obj.kvDirty) != 1 {
		t.Fatal("first account KV write did not initialize the dirty map")
	}
}

func TestNewEmptyStateObjectDefaultsKVFields(t *testing.T) {
	var addr tcommon.Address
	addr[0] = tcommon.AddressPrefixMainnet
	obj := newEmptyStateObject(addr)
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
	if obj.storage != nil || obj.kvDirty != nil {
		t.Fatal("new empty state object eagerly allocated storage maps")
	}
}

func TestStateObjectReleaseKVDirtyClearsLifecycleState(t *testing.T) {
	obj := new(stateObject)
	obj.stageKV(kvdomains.ContractMetadata, []byte("old"), []byte("value"))
	if obj.kvDirtyHighWater != 1 {
		t.Fatalf("high water = %d, want 1", obj.kvDirtyHighWater)
	}
	obj.releaseKVDirty()
	if obj.kvDirty != nil || obj.kvDirtyHighWater != 0 {
		t.Fatalf("released dirty state = (%v,%d), want (nil,0)", obj.kvDirty, obj.kvDirtyHighWater)
	}

	obj.stageKV(kvdomains.ContractMetadata, []byte("new"), []byte("next"))
	if len(obj.kvDirty) != 1 {
		t.Fatalf("reused dirty map length = %d, want 1", len(obj.kvDirty))
	}
	if _, stale := lookupKVEntry(obj.kvDirty, kvdomains.ContractMetadata, []byte("old")); stale {
		t.Fatal("reused dirty map retained an entry from its previous lifecycle")
	}
	obj.releaseKVDirty()
}

func TestStateDBCopyPreservesLazyStateMaps(t *testing.T) {
	addr := tcommon.BytesToAddress([]byte{3})
	original := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Normal))
	sdb := &StateDB{
		stateObjects: map[tcommon.Address]*stateObject{addr: original},
	}

	copyState, err := sdb.Copy()
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	copied := copyState.stateObjects[addr]
	if copied == nil {
		t.Fatal("copied state object missing")
	}
	if copied.storage != nil || copied.dirtyStorage != nil || copied.kvDirty != nil {
		t.Fatal("Copy eagerly allocated empty state maps")
	}
}
