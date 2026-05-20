package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	if hash := sdb.GetCodeHash(addr); hash != tcommon.Keccak256(code) {
		t.Fatalf("code hash: got %x, want %x", hash, tcommon.Keccak256(code))
	}
}

func TestStateDBCodeHashForExistingEmptyAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := tcommon.Address{0x41, 0x01}
	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	if hash := sdb.GetCodeHash(addr); hash != tcommon.Keccak256(nil) {
		t.Fatalf("empty account code hash: got %x, want %x", hash, tcommon.Keccak256(nil))
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

func TestStateDBSelfDestructDeletesAccountAtCommit(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x34}
	code := []byte{0x60, 0x2a, 0x60, 0x00, 0x52, 0x00}
	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "selfdestructed",
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	sdb.SetContract(addr, meta)
	if err := rawdb.WriteContractABI(diskdb, addr.Bytes(), &contractpb.SmartContract_ABI{}); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SelfDestruct(addr)
	if !sdb.AccountExists(addr) {
		t.Fatal("selfdestruct should not hide account before commit")
	}
	if got := sdb.GetCode(addr); string(got) != string(code) {
		t.Fatalf("selfdestruct should not hide code before commit: got %x", got)
	}
	if got := sdb.GetContract(addr); got == nil || got.Name != "selfdestructed" {
		t.Fatalf("selfdestruct should not hide contract meta before commit: %+v", got)
	}

	root, err = sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if sdb.AccountExists(addr) {
		t.Fatal("selfdestructed account survived commit")
	}
	if got := sdb.GetCode(addr); len(got) != 0 {
		t.Fatalf("selfdestructed code survived commit: %x", got)
	}
	if got := sdb.GetContract(addr); got != nil {
		t.Fatalf("selfdestructed contract meta survived commit: %+v", got)
	}
	if rawdb.ReadContractABI(diskdb, addr.Bytes()) != nil {
		t.Fatal("selfdestructed dedicated ABI survived commit")
	}
}

func TestStateDBFinalizeTransactionDeletesSelfDestructForNextTx(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x44}
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "finalize-selfdestruct",
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	sdb.SetContract(addr, meta)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	sdb.SelfDestruct(addr)
	sdb.FinalizeTransaction()
	if sdb.AccountExists(addr) {
		t.Fatal("selfdestructed account should be absent after transaction boundary")
	}
	if got := sdb.GetCodeSize(addr); got != 0 {
		t.Fatalf("code should be hidden after transaction boundary: got size %d", got)
	}
	if sdb.GetContract(addr) != nil {
		t.Fatal("contract metadata should be hidden after transaction boundary")
	}

	snap := sdb.Snapshot()
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, []byte{0x00})
	sdb.RevertToSnapshot(snap)
	if sdb.AccountExists(addr) {
		t.Fatal("reverting a later recreate should restore the pending delete")
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	if sdb.AccountExists(addr) {
		t.Fatal("pending delete should survive commit after later recreate revert")
	}
}

func TestDeleteAccountClearsPersistedContractCodeOnRecreate(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := tcommon.Address{0x41, 0x44}
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}
	meta := &contractpb.SmartContract{
		OriginAddress:   addr.Bytes(),
		ContractAddress: addr.Bytes(),
		Name:            "deleted",
	}

	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetCode(addr, code)
	sdb.SetContract(addr, meta)
	if err := rawdb.WriteContractABI(diskdb, addr.Bytes(), &contractpb.SmartContract_ABI{}); err != nil {
		t.Fatal(err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetCode(addr); string(got) != string(code) {
		t.Fatalf("precondition code mismatch: got %x", got)
	}
	if got := sdb.GetContract(addr); got == nil || got.Name != "deleted" {
		t.Fatalf("precondition contract meta mismatch: %+v", got)
	}

	sdb.DeleteAccount(addr)
	sdb.CreateAccountWithTime(addr, corepb.AccountType_Normal, 12345)
	if got := sdb.GetCode(addr); len(got) != 0 {
		t.Fatalf("recreated normal account must not expose stale code before commit: %x", got)
	}
	if got := sdb.GetContract(addr); got != nil {
		t.Fatalf("recreated normal account must not expose stale contract meta before commit: %+v", got)
	}
	root, err = sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetCode(addr); len(got) != 0 {
		t.Fatalf("stale code survived commit: %x", got)
	}
	if got := sdb.GetContract(addr); got != nil {
		t.Fatalf("stale contract meta survived commit: %+v", got)
	}
	if rawdb.ReadContractABI(diskdb, addr.Bytes()) != nil {
		t.Fatal("stale dedicated ABI survived commit")
	}
	if !sdb.AccountExists(addr) {
		t.Fatal("recreated normal account should persist")
	}
	if sdb.GetAccount(addr).Type() != corepb.AccountType_Normal {
		t.Fatalf("account type: got %v, want normal", sdb.GetAccount(addr).Type())
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
