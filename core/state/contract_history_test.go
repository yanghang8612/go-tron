package state

import (
	"bytes"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestContractAtSurfacesCorruptMetadata(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x71)

	if err := rawdb.WriteStateTxRange(db, 1, tcommon.Hash{0x01}, 1, 1); err != nil {
		t.Fatalf("write block 1 tx range: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{0x02}, 2, 2); err != nil {
		t.Fatalf("write block 2 tx range: %v", err)
	}
	if err := rawdb.WriteStateKVGeneration(db, addr, 0); err != nil {
		t.Fatalf("write kv generation: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, addr, 0, kvdomains.ContractMetadata, contractMetaKVKey, []byte{0x80}); err != nil {
		t.Fatalf("write corrupt contract metadata: %v", err)
	}

	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, &keyedColdHistoryStub{})
	got, err := reader.ContractAt(addr, 1)
	if err == nil {
		t.Fatal("ContractAt corrupt metadata error = nil")
	}
	if got != nil {
		t.Fatalf("ContractAt corrupt metadata contract = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "decode contract metadata at block 1") {
		t.Fatalf("ContractAt corrupt metadata error = %v, want decode contract metadata context", err)
	}
}

func TestContractAtUsesHistoricalAccountKVGeneration(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x72)
	oldAccount := mustContractHistoryAccountEnvelope(t, 0)
	newAccount := mustContractHistoryAccountEnvelope(t, 1)
	oldMeta := mustContractHistoryMetadata(t, addr, "old-contract", []byte{0x01, 0x02})
	newMeta := mustContractHistoryMetadata(t, addr, "new-contract", []byte{0x03, 0x04})

	if err := rawdb.WriteStateTxRange(db, 1, tcommon.Hash{0x01}, 1, 1); err != nil {
		t.Fatalf("write block 1 tx range: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{0x02}, 2, 2); err != nil {
		t.Fatalf("write block 2 tx range: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(db, addr, newAccount); err != nil {
		t.Fatalf("write latest account: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, addr, 1, kvdomains.ContractMetadata, contractMetaKVKey, newMeta); err != nil {
		t.Fatalf("write latest new metadata: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      addr,
		PrevExists: true,
		Prev:       oldAccount,
		NextExists: true,
		Next:       newAccount,
	}); err != nil {
		t.Fatalf("write account change: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        2,
		FlatDomain: rawdb.StateFlatDomainKVGeneration,
		Owner:      addr,
		PrevExists: true,
		Prev:       rawdb.EncodeStateKVGenerationValue(0),
		NextExists: true,
		Next:       rawdb.EncodeStateKVGenerationValue(1),
	}); err != nil {
		t.Fatalf("write generation change: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        3,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      addr,
		Generation: 0,
		Domain:     kvdomains.ContractMetadata,
		Key:        contractMetaKVKey,
		PrevExists: true,
		Prev:       oldMeta,
		NextExists: false,
	}); err != nil {
		t.Fatalf("write old metadata change: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  tcommon.Hash{0x02},
		TxNum:      2,
		Seq:        4,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      addr,
		Generation: 1,
		Domain:     kvdomains.ContractMetadata,
		Key:        contractMetaKVKey,
		PrevExists: false,
		NextExists: true,
		Next:       newMeta,
	}); err != nil {
		t.Fatalf("write new metadata change: %v", err)
	}

	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, &keyedColdHistoryStub{})
	gotOld, err := reader.ContractAt(addr, 1)
	if err != nil {
		t.Fatalf("ContractAt(block1): %v", err)
	}
	if gotOld == nil || gotOld.Name != "old-contract" || !bytes.Equal(gotOld.Bytecode, []byte{0x01, 0x02}) {
		t.Fatalf("ContractAt(block1) = %+v, want old-contract", gotOld)
	}
	gotNew, err := reader.ContractAt(addr, 2)
	if err != nil {
		t.Fatalf("ContractAt(block2): %v", err)
	}
	if gotNew == nil || gotNew.Name != "new-contract" || !bytes.Equal(gotNew.Bytecode, []byte{0x03, 0x04}) {
		t.Fatalf("ContractAt(block2) = %+v, want new-contract", gotNew)
	}
}

func mustContractHistoryAccountEnvelope(t *testing.T, generation uint64) []byte {
	t.Helper()
	envelope := &StateAccountV2{
		Version:             StateAccountVersion,
		AccountKVRoot:       EmptyKVRoot,
		AccountKVGeneration: generation,
	}
	raw, err := envelope.Encode()
	if err != nil {
		t.Fatalf("encode account envelope: %v", err)
	}
	return raw
}

func mustContractHistoryMetadata(t *testing.T, addr tcommon.Address, name string, bytecode []byte) []byte {
	t.Helper()
	raw, err := statecodec.Marshal(&contractpb.SmartContract{
		ContractAddress: addr.Bytes(),
		Name:            name,
		Bytecode:        bytecode,
	})
	if err != nil {
		t.Fatalf("marshal contract metadata: %v", err)
	}
	return raw
}
