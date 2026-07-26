package rawdb

import (
	"bytes"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestWriteReadTransactionInfo(t *testing.T) {
	db := NewMemoryChainDB()

	txID := bytes.Repeat([]byte{0xAB}, 32)
	info := &corepb.TransactionInfo{
		Id:             txID,
		Fee:            12345,
		BlockNumber:    100,
		BlockTimeStamp: 300000,
		Receipt: &corepb.ResourceReceipt{
			EnergyUsage:      500,
			EnergyFee:        50000,
			EnergyUsageTotal: 500,
		},
	}

	WriteTransactionInfo(db, txID, info)

	got := ReadTransactionInfo(db, txID)
	if got == nil {
		t.Fatal("expected non-nil TransactionInfo")
	}
	if got.Fee != 12345 {
		t.Fatalf("fee: got %d, want 12345", got.Fee)
	}
	if got.BlockNumber != 100 {
		t.Fatalf("blockNumber: got %d, want 100", got.BlockNumber)
	}
	if got.Receipt.EnergyUsage != 500 {
		t.Fatalf("energyUsage: got %d, want 500", got.Receipt.EnergyUsage)
	}
}

func TestReadTransactionInfo_NotFound(t *testing.T) {
	db := NewMemoryChainDB()
	got := ReadTransactionInfo(db, bytes.Repeat([]byte{0x00}, 32))
	if got != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestReadTransactionInfoFromHotBlockRow(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x31}, 32)
	otherID := bytes.Repeat([]byte{0x32}, 32)
	infos := []*corepb.TransactionInfo{
		{Id: otherID, Fee: 1, BlockNumber: 17},
		{Id: txID, Fee: 9876, BlockNumber: 17},
	}
	if err := WriteTransactionIndex(db, txID, 17); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransactionInfosByBlock(db, 17, infos); err != nil {
		t.Fatal(err)
	}
	if has, err := db.Has(txInfoKey(txID)); err != nil || has {
		t.Fatalf("legacy row exists=%v err=%v", has, err)
	}

	got := ReadTransactionInfo(db, txID)
	if got == nil || got.Fee != 9876 || !bytes.Equal(got.Id, txID) {
		t.Fatalf("derived info = %#v", got)
	}
}

func TestFindTransactionInfoInRetHandlesUnknownFieldsAndMalformedWire(t *testing.T) {
	txID := bytes.Repeat([]byte{0x41}, 32)
	infoData, err := proto.Marshal(&corepb.TransactionInfo{Id: txID, Fee: 55})
	if err != nil {
		t.Fatal(err)
	}
	unknown := protowire.AppendTag(nil, 99, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 7)
	infoData = append(unknown, infoData...)
	retData := protowire.AppendTag(nil, 3, protowire.BytesType)
	retData = protowire.AppendBytes(retData, infoData)

	got := findTransactionInfoInRet(retData, txID)
	if got == nil || got.Fee != 55 {
		t.Fatalf("wire lookup = %#v", got)
	}
	if got := findTransactionInfoInRet([]byte{0x1a, 0xff}, txID); got != nil {
		t.Fatalf("malformed wire returned %#v", got)
	}
}

func TestDeleteLegacyTransactionInfosPreservesAdjacentKeyspaces(t *testing.T) {
	db := NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })
	txID := bytes.Repeat([]byte{0x51}, 32)
	if err := db.Put(txInfoKey(txID), []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(txInfoBlockKey(9), []byte("block-ret")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(txKey(txID), make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if has, err := HasLegacyTransactionInfos(db); err != nil || !has {
		t.Fatalf("before prune exists=%v err=%v", has, err)
	}
	if err := DeleteLegacyTransactionInfos(db); err != nil {
		t.Fatal(err)
	}
	if has, err := HasLegacyTransactionInfos(db); err != nil || has {
		t.Fatalf("after prune exists=%v err=%v", has, err)
	}
	if has, err := db.Has(txInfoBlockKey(9)); err != nil || !has {
		t.Fatalf("tib row exists=%v err=%v", has, err)
	}
	if has, err := db.Has(txKey(txID)); err != nil || !has {
		t.Fatalf("tx row exists=%v err=%v", has, err)
	}
}

func TestWriteReadTransactionInfosByBlock(t *testing.T) {
	db := NewMemoryChainDB()

	infos := []*corepb.TransactionInfo{
		{Id: bytes.Repeat([]byte{0x01}, 32), Fee: 100, BlockNumber: 5, BlockTimeStamp: 15000},
		{Id: bytes.Repeat([]byte{0x02}, 32), Fee: 200, BlockNumber: 5, BlockTimeStamp: 15000},
	}

	WriteTransactionInfosByBlock(db, 5, infos)

	got := ReadTransactionInfosByBlock(db, 5)
	if len(got) != 2 {
		t.Fatalf("expected 2 infos, got %d", len(got))
	}
	if got[0].Fee != 100 {
		t.Fatalf("info[0] fee: got %d, want 100", got[0].Fee)
	}
	if got[1].Fee != 200 {
		t.Fatalf("info[1] fee: got %d, want 200", got[1].Fee)
	}
}

func TestReadTransactionInfosByBlock_NotFound(t *testing.T) {
	db := NewMemoryChainDB()
	got := ReadTransactionInfosByBlock(db, 999)
	if len(got) != 0 {
		t.Fatalf("expected 0 infos, got %d", len(got))
	}
}

func TestWriteReadTransactionIndex(t *testing.T) {
	db := NewMemoryChainDB()

	txHash := bytes.Repeat([]byte{0xCC}, 32)
	WriteTransactionIndex(db, txHash, 42)

	got := ReadTransactionIndex(db, txHash)
	if got == nil {
		t.Fatal("expected non-nil block number")
	}
	if *got != 42 {
		t.Fatalf("block number: got %d, want 42", *got)
	}
}

func TestReadTransactionIndex_NotFound(t *testing.T) {
	db := NewMemoryChainDB()
	got := ReadTransactionIndex(db, bytes.Repeat([]byte{0x00}, 32))
	if got != nil {
		t.Fatal("expected nil for missing tx index")
	}
}
