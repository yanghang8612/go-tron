package state

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// TestContractMetaSnapshotRevert verifies that a contractMeta mutation can be
// reverted via RevertToSnapshot. This guards against journal aliasing bugs where
// the pre-mutation snapshot is lost because the same proto pointer is shared.
func TestContractMetaSnapshotRevert(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	contractAddr := testAddr(9)
	originAddr := testAddr(8)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:              originAddr[:],
		ConsumeUserResourcePercent: 10,
	})

	snap := sdb.Snapshot()

	// Actuator pattern: clone before mutating so journal can capture pre-mutation state.
	raw := sdb.GetContract(contractAddr)
	mutated := proto.Clone(raw).(*contractpb.SmartContract)
	mutated.ConsumeUserResourcePercent = 99
	sdb.SetContract(contractAddr, mutated)

	if got := sdb.GetContract(contractAddr); got.ConsumeUserResourcePercent != 99 {
		t.Fatalf("mutation not applied: got %d", got.ConsumeUserResourcePercent)
	}

	sdb.RevertToSnapshot(snap)

	reverted := sdb.GetContract(contractAddr)
	if reverted == nil {
		t.Fatal("contract nil after revert")
	}
	if reverted.ConsumeUserResourcePercent != 10 {
		t.Fatalf("revert failed: ConsumeUserResourcePercent = %d, want 10", reverted.ConsumeUserResourcePercent)
	}
}

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
	hash := tcommon.Keccak256(code)
	if flat := rawdb.ReadCode(diskdb, contractAddr); len(flat) != 0 {
		t.Fatalf("legacy flat code mirror was written: %x", flat)
	}
	if got := rawdb.ReadStateCode(diskdb, hash); !bytes.Equal(got, code) {
		t.Fatalf("state code domain = %x, want %x", got, code)
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
	raw, err := sdb2.trie.Get(trieKey(contractAddr))
	if err != nil || raw == nil {
		t.Fatalf("trie.Get: data=%v err=%v", raw, err)
	}
	envelope, err := DecodeStateAccountV2(raw)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.CodeHash != hash {
		t.Fatalf("envelope CodeHash = %x, want %x", envelope.CodeHash, hash)
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
