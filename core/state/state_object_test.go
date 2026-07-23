package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestStateObjectCodeStorage(t *testing.T) {
	addr := tcommon.Address{0x41, 1}
	obj := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Contract))

	if obj.code != nil {
		t.Fatal("expected nil code initially")
	}

	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	obj.setCode(code)

	if string(obj.code) != string(code) {
		t.Fatalf("code mismatch: got %x, want %x", obj.code, code)
	}
	if !obj.dirty {
		t.Fatal("expected dirty after setCode")
	}
	if obj.codeHash != tcommon.Keccak256(code) {
		t.Fatalf("codeHash: got %x, want %x", obj.codeHash, tcommon.Keccak256(code))
	}
}

func TestStateObjectContractStorage(t *testing.T) {
	addr := tcommon.Address{0x41, 1}
	obj := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Contract))

	key := tcommon.Hash{0x01}
	val := tcommon.Hash{0x42}

	got := obj.getStorage(key)
	if got != (tcommon.Hash{}) {
		t.Fatalf("expected empty storage, got %x", got)
	}

	obj.setStorage(key, val, true)
	got, exists, cached := obj.getStorageWithExist(key)
	if got != val || !exists || !cached {
		t.Fatalf("storage mismatch: got (%x,%v,%v), want (%x,true,true)", got, exists, cached, val)
	}

	obj.setStorage(key, tcommon.Hash{}, false)
	got, exists, cached = obj.getStorageWithExist(key)
	if got != (tcommon.Hash{}) || exists || !cached {
		t.Fatalf("cached absent storage mismatch: got (%x,%v,%v), want (zero,false,true)", got, exists, cached)
	}
}

func TestStateObjectSelfDestruct(t *testing.T) {
	addr := tcommon.Address{0x41, 1}
	obj := newStateObject(addr, types.NewAccount(addr, corepb.AccountType_Contract))
	obj.setCode([]byte{0x00})

	if obj.selfDestructed {
		t.Fatal("should not be selfDestructed initially")
	}

	obj.markSelfDestructed()
	if !obj.selfDestructed {
		t.Fatal("should be selfDestructed after mark")
	}
}
