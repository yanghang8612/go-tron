package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
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
}
