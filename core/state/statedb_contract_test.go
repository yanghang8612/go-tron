package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestStateDBCodeMethods(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x01}

	if code := sdb.GetCode(addr); code != nil {
		t.Fatalf("expected nil code, got %x", code)
	}
	if size := sdb.GetCodeSize(addr); size != 0 {
		t.Fatalf("expected 0 code size, got %d", size)
	}

	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	sdb.SetCode(addr, code)

	if got := sdb.GetCode(addr); string(got) != string(code) {
		t.Fatalf("code mismatch: got %x, want %x", got, code)
	}
	if size := sdb.GetCodeSize(addr); size != len(code) {
		t.Fatalf("code size mismatch: got %d, want %d", size, len(code))
	}
	if hash := sdb.GetCodeHash(addr); hash == (tcommon.Hash{}) {
		t.Fatal("expected non-empty code hash")
	}
}

func TestStateDBStorageMethods(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x02}

	key := tcommon.Hash{0x01}
	val := tcommon.Hash{0x42}

	if got := sdb.GetState(addr, key); got != (tcommon.Hash{}) {
		t.Fatalf("expected empty state, got %x", got)
	}

	sdb.SetState(addr, key, val)
	if got := sdb.GetState(addr, key); got != val {
		t.Fatalf("state mismatch: got %x, want %x", got, val)
	}
}

func TestStateDBContractMeta(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x03}

	if sdb.IsContract(addr) {
		t.Fatal("should not be contract initially")
	}

	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "test",
	}
	sdb.SetContract(addr, meta)
	if !sdb.IsContract(addr) {
		t.Fatal("should be contract after SetContract")
	}
	got := sdb.GetContract(addr)
	if got == nil || got.Name != "test" {
		t.Fatal("contract meta mismatch")
	}
}

func TestStateDBSelfDestruct(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x04}

	sdb.SetCode(addr, []byte{0x00})
	if sdb.HasSelfDestructed(addr) {
		t.Fatal("should not be selfDestructed")
	}

	sdb.SelfDestruct(addr)
	if !sdb.HasSelfDestructed(addr) {
		t.Fatal("should be selfDestructed")
	}
}

func TestStateDBExistEmpty(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x05}

	if sdb.Exist(addr) {
		t.Fatal("should not exist")
	}
	if !sdb.Empty(addr) {
		t.Fatal("should be empty")
	}

	sdb.AddBalance(addr, 100)
	if !sdb.Exist(addr) {
		t.Fatal("should exist after AddBalance")
	}
	if sdb.Empty(addr) {
		t.Fatal("should not be empty with balance")
	}
}

func TestStateDBStorageRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x06}
	key := tcommon.Hash{0x01}

	sdb.SetState(addr, key, tcommon.Hash{0x10})
	snap := sdb.Snapshot()
	sdb.SetState(addr, key, tcommon.Hash{0x20})

	if got := sdb.GetState(addr, key); got != (tcommon.Hash{0x20}) {
		t.Fatalf("expected 0x20, got %x", got)
	}

	sdb.RevertToSnapshot(snap)
	if got := sdb.GetState(addr, key); got != (tcommon.Hash{0x10}) {
		t.Fatalf("expected 0x10 after revert, got %x", got)
	}
}

func TestStateDBCopy(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x07}
	sdb.AddBalance(addr, 1000)
	sdb.SetCode(addr, []byte{0x60, 0x00})
	sdb.SetState(addr, tcommon.Hash{0x01}, tcommon.Hash{0x42})

	cp, err := sdb.Copy()
	if err != nil {
		t.Fatal(err)
	}

	// Verify copy has same data
	if cp.GetBalance(addr) != 1000 {
		t.Fatal("copy balance mismatch")
	}
	if string(cp.GetCode(addr)) != string(sdb.GetCode(addr)) {
		t.Fatal("copy code mismatch")
	}
	if cp.GetState(addr, tcommon.Hash{0x01}) != (tcommon.Hash{0x42}) {
		t.Fatal("copy storage mismatch")
	}

	// Modify copy, original unchanged
	cp.AddBalance(addr, 500)
	if sdb.GetBalance(addr) != 1000 {
		t.Fatal("original should be unchanged")
	}
}
