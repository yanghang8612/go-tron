package rawdb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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

func TestReadTransactionInfoRejectsMismatchedHotRow(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x11}, common.HashLength)
	otherID := bytes.Repeat([]byte{0x12}, common.HashLength)
	data, err := proto.Marshal(&corepb.TransactionInfo{Id: otherID, Fee: 99})
	if err != nil {
		t.Fatalf("marshal tx info: %v", err)
	}
	if err := db.Put(txInfoKey(txID), data); err != nil {
		t.Fatalf("put mismatched tx info: %v", err)
	}
	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo mismatched hot row = %+v, want nil", got)
	}
}

func TestWriteTransactionInfoRejectsMismatchedID(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x21}, common.HashLength)
	otherID := bytes.Repeat([]byte{0x22}, common.HashLength)
	if err := WriteTransactionInfo(db, txID, &corepb.TransactionInfo{Id: otherID, Fee: 99}); err == nil {
		t.Fatal("WriteTransactionInfo accepted mismatched transaction id")
	}
	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo after rejected write = %+v, want nil", got)
	}
}

func TestReadTransactionInfoUsesColdTxPositionWhenInfoIDMissing(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x34}, common.HashLength)
	infos := []*corepb.TransactionInfo{
		{Fee: 100, BlockNumber: 7, BlockTimeStamp: 7000},
		{Fee: 200, BlockNumber: 7, BlockTimeStamp: 7000},
	}
	if err := WriteTransactionInfosByBlock(db, 7, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	var hash common.Hash
	copy(hash[:], txID)
	db.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{hash: 7},
		positions: map[common.Hash]ChainIndexTxLookup{
			hash: {BlockNum: 7, TxIndex: 1},
		},
	})

	got := ReadTransactionInfo(db, txID)
	if got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfo via cold tx position = %+v, want fee 200", got)
	}
}

func TestReadTransactionInfoRejectsMismatchedColdTxPosition(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x35}, common.HashLength)
	otherID := bytes.Repeat([]byte{0x36}, common.HashLength)
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Id: otherID, Fee: 999, BlockNumber: 7, BlockTimeStamp: 7000},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	var hash common.Hash
	copy(hash[:], txID)
	db.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{hash: 7},
		positions: map[common.Hash]ChainIndexTxLookup{
			hash: {BlockNum: 7, TxIndex: 0},
		},
	})

	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo mismatched cold position = %+v, want nil", got)
	}
}

func TestReadTransactionInfoDoesNotScanAfterMismatchedColdTxPosition(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x39}, common.HashLength)
	otherID := bytes.Repeat([]byte{0x3a}, common.HashLength)
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Id: txID, Fee: 111, BlockNumber: 7, BlockTimeStamp: 7000},
		{Id: otherID, Fee: 222, BlockNumber: 7, BlockTimeStamp: 7000},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	var hash common.Hash
	copy(hash[:], txID)
	db.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{hash: 7},
		positions: map[common.Hash]ChainIndexTxLookup{
			hash: {BlockNum: 7, TxIndex: 1},
		},
	})

	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo scanned past mismatched cold position = %+v, want nil", got)
	}
}

func TestReadTransactionInfoRejectsColdBlockNumberMismatch(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x37}, common.HashLength)
	writeRawTransactionRetForTest(t, db, 7, &corepb.TransactionRet{
		BlockNumber: 7,
		Transactioninfo: []*corepb.TransactionInfo{
			{Fee: 999, BlockNumber: 8, BlockTimeStamp: 7000},
		},
	})
	var hash common.Hash
	copy(hash[:], txID)
	db.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{hash: 7},
		positions: map[common.Hash]ChainIndexTxLookup{
			hash: {BlockNum: 7, TxIndex: 0},
		},
	})
	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo cold block mismatch by position = %+v, want nil", got)
	}

	txID2 := bytes.Repeat([]byte{0x38}, common.HashLength)
	writeRawTransactionRetForTest(t, db, 9, &corepb.TransactionRet{
		BlockNumber: 9,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: txID2, Fee: 111, BlockNumber: 10, BlockTimeStamp: 9000},
		},
	})
	if err := WriteTransactionIndex(db, txID2, 9); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if got := ReadTransactionInfo(db, txID2); got != nil {
		t.Fatalf("ReadTransactionInfo cold block mismatch by scan = %+v, want nil", got)
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

func TestReadTransactionInfosByBlockAcceptsLegacyZeroBlockNumber(t *testing.T) {
	db := NewMemoryChainDB()
	data, err := proto.Marshal(&corepb.TransactionRet{
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: bytes.Repeat([]byte{0x03}, 32), Fee: 300},
		},
	})
	if err != nil {
		t.Fatalf("marshal legacy ret: %v", err)
	}
	if err := db.Put(txInfoBlockKey(5), data); err != nil {
		t.Fatalf("put legacy ret: %v", err)
	}
	got := ReadTransactionInfosByBlock(db, 5)
	if len(got) != 1 || got[0].Fee != 300 {
		t.Fatalf("legacy zero block number read = %+v, want one fee 300", got)
	}
}

func TestReadTransactionInfosByBlockRejectsMismatchedRetBlockNumber(t *testing.T) {
	db := NewMemoryChainDB()
	data, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber: 6,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: bytes.Repeat([]byte{0x04}, 32), Fee: 400, BlockNumber: 5},
		},
	})
	if err != nil {
		t.Fatalf("marshal mismatched ret: %v", err)
	}
	if err := db.Put(txInfoBlockKey(5), data); err != nil {
		t.Fatalf("put mismatched ret: %v", err)
	}
	if got := ReadTransactionInfosByBlock(db, 5); got != nil {
		t.Fatalf("mismatched TransactionRet block number read = %+v, want nil", got)
	}
}

func TestReadTransactionInfosByBlockRejectsMismatchedInfoBlockNumber(t *testing.T) {
	db := NewMemoryChainDB()
	data, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber: 5,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: bytes.Repeat([]byte{0x05}, 32), Fee: 500, BlockNumber: 6},
		},
	})
	if err != nil {
		t.Fatalf("marshal mismatched info: %v", err)
	}
	if err := db.Put(txInfoBlockKey(5), data); err != nil {
		t.Fatalf("put mismatched info: %v", err)
	}
	if got := ReadTransactionInfosByBlock(db, 5); got != nil {
		t.Fatalf("mismatched TransactionInfo block number read = %+v, want nil", got)
	}
}

func TestReadTransactionInfosByBlockStrictReportsMismatchedPayload(t *testing.T) {
	db := NewMemoryChainDB()
	data, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber: 5,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: bytes.Repeat([]byte{0x06}, 32), Fee: 600, BlockNumber: 6},
		},
	})
	if err != nil {
		t.Fatalf("marshal mismatched strict ret: %v", err)
	}
	if err := db.Put(txInfoBlockKey(5), data); err != nil {
		t.Fatalf("put mismatched strict ret: %v", err)
	}
	infos, ok, err := ReadTransactionInfosByBlockStrict(db, 5)
	if err == nil || !strings.Contains(err.Error(), "transaction info block number 6") {
		t.Fatalf("strict tx-info read = %+v/%v/%v, want block number mismatch error", infos, ok, err)
	}
	if !ok {
		t.Fatal("strict tx-info read reported missing source row for mismatched payload")
	}
}

func TestReadTransactionInfosByBlockStrictReportsMissingSource(t *testing.T) {
	db := NewMemoryChainDB()
	infos, ok, err := ReadTransactionInfosByBlockStrict(db, 5)
	if err != nil || ok || infos != nil {
		t.Fatalf("strict missing tx-info read = %+v/%v/%v, want nil/false/nil", infos, ok, err)
	}
}

func TestWriteTransactionInfosByBlockRejectsNilEntry(t *testing.T) {
	db := NewMemoryChainDB()
	err := WriteTransactionInfosByBlock(db, 5, []*corepb.TransactionInfo{nil})
	if err == nil {
		t.Fatal("WriteTransactionInfosByBlock accepted nil transaction info")
	}
}

func TestWriteTransactionInfosByBlockRejectsMismatchedBlockNumber(t *testing.T) {
	db := NewMemoryChainDB()
	err := WriteTransactionInfosByBlock(db, 5, []*corepb.TransactionInfo{{BlockNumber: 6}})
	if err == nil || !strings.Contains(err.Error(), "transaction info block number 6") {
		t.Fatalf("WriteTransactionInfosByBlock err = %v, want block number mismatch", err)
	}
	if got := ReadTransactionInfosByBlock(db, 5); got != nil {
		t.Fatalf("ReadTransactionInfosByBlock after rejected write = %+v, want nil", got)
	}
}

func TestReadTransactionInfosByBlock_NotFound(t *testing.T) {
	db := NewMemoryChainDB()
	got := ReadTransactionInfosByBlock(db, 999)
	if len(got) != 0 {
		t.Fatalf("expected 0 infos, got %d", len(got))
	}
}

func writeRawTransactionRetForTest(t *testing.T, db *ChainDB, blockNum uint64, ret *corepb.TransactionRet) {
	t.Helper()
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal raw TransactionRet: %v", err)
	}
	if err := db.Put(txInfoBlockKey(blockNum), data); err != nil {
		t.Fatalf("put raw TransactionRet: %v", err)
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
