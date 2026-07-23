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
