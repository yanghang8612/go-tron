package state

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestContractCodePersistence(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	contractAddr := testAddr(10)
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}

	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.SetCode(contractAddr, code)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Open a fresh StateDB — empty in-memory cache, same disk storage
	db2 := NewDatabase(diskdb)
	sdb2, err := New(root, db2)
	if err != nil {
		t.Fatal(err)
	}

	got := sdb2.GetCode(contractAddr)
	if !bytes.Equal(got, code) {
		t.Fatalf("code not persisted after restart: got %x", got)
	}
}

func TestContractMetaPersistence(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	owner := testAddr(11)
	contractAddr := testAddr(12)
	meta := &contractpb.SmartContract{
		OriginAddress:              owner[:],
		Name:                       "PersistTest",
		ConsumeUserResourcePercent: 50,
	}

	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.SetContract(contractAddr, meta)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	db2 := NewDatabase(diskdb)
	sdb2, err := New(root, db2)
	if err != nil {
		t.Fatal(err)
	}

	gotMeta := sdb2.GetContract(contractAddr)
	if gotMeta == nil {
		t.Fatal("contract metadata not persisted after restart")
	}
	if gotMeta.Name != "PersistTest" {
		t.Fatalf("wrong name: %s", gotMeta.Name)
	}
	if gotMeta.ConsumeUserResourcePercent != 50 {
		t.Fatalf("wrong consume percent: %d", gotMeta.ConsumeUserResourcePercent)
	}
}
