package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
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
	if has, err := HasHotTransactionInfo(db, txID); err != nil || !has {
		t.Fatalf("HasHotTransactionInfo = %v/%v, want true/nil", has, err)
	}
}

func TestHasHotTransactionInfoRejectsMalformedKey(t *testing.T) {
	db := NewMemoryChainDB()
	if has, err := HasHotTransactionInfo(db, []byte{0x01}); err == nil || has || !strings.Contains(err.Error(), "transaction hash length 1") {
		t.Fatalf("HasHotTransactionInfo malformed key = %v/%v, want false/hash length error", has, err)
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
	if info, ok, err := ReadTransactionInfoStrict(db, txID); err == nil || !ok || info == nil {
		t.Fatalf("ReadTransactionInfoStrict mismatched hot row = info %+v ok %v err %v, want info/ok/error", info, ok, err)
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

func TestWriteTransactionInfoRejectsMalformedKey(t *testing.T) {
	db := NewMemoryChainDB()
	txID := []byte{0x21}
	if err := WriteTransactionInfo(db, txID, &corepb.TransactionInfo{Fee: 99}); err == nil || !strings.Contains(err.Error(), "transaction hash length 1") {
		t.Fatalf("WriteTransactionInfo malformed key err = %v, want hash length error", err)
	}
	if _, err := db.Get(txInfoKey(txID)); err == nil {
		t.Fatal("WriteTransactionInfo malformed key created a raw row")
	}
	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo malformed key = %+v, want nil", got)
	}
}

func TestReadTransactionInfoRejectsMalformedKey(t *testing.T) {
	db := NewMemoryChainDB()
	txID := []byte{0x23}
	data, err := proto.Marshal(&corepb.TransactionInfo{Fee: 99})
	if err != nil {
		t.Fatalf("marshal tx info: %v", err)
	}
	if err := db.Put(txInfoKey(txID), data); err != nil {
		t.Fatalf("put malformed tx info key: %v", err)
	}
	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo malformed key = %+v, want nil", got)
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

func TestReadTransactionInfoColdPositionChecksReadableBlockBody(t *testing.T) {
	db := NewMemoryChainDB()
	txPB1 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7001, Expiration: 8001, Data: []byte{0x01}}}
	txPB2 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7002, Expiration: 8002, Data: []byte{0x02}}}
	txHash2 := types.NewTransactionFromPB(txPB2).Hash()
	blockPB := newBlockProto(7, 7000)
	blockPB.Transactions = []*corepb.Transaction{txPB1, txPB2}
	block := types.NewBlockFromPB(blockPB)
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Fee: 100, BlockNumber: 7, BlockTimeStamp: 7000},
		{Fee: 200, BlockNumber: 7, BlockTimeStamp: 7000},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	db.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{txHash2: 7},
		positions: map[common.Hash]ChainIndexTxLookup{
			txHash2: {BlockNum: 7, TxIndex: 1},
		},
	})

	got := ReadTransactionInfo(db, txHash2[:])
	if got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfo checked cold tx position = %+v, want fee 200", got)
	}
}

func TestReadTransactionInfoColdPositionRejectsCorruptReadableBlockBody(t *testing.T) {
	ancient := newFakeAncient()
	ancient.put(ancientBlocks, 7, []byte("not-a-valid-block"))
	db := NewChainDB(NewMemoryDatabase(), ancient)
	txID := bytes.Repeat([]byte{0x3b}, common.HashLength)
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Fee: 200, BlockNumber: 7, BlockTimeStamp: 7000},
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
		t.Fatalf("ReadTransactionInfo corrupt readable block body = %+v, want nil", got)
	}
	if info, ok, err := ReadTransactionInfoStrict(db, txID); err == nil || !ok || info != nil || !strings.Contains(err.Error(), "block 7 decode") {
		t.Fatalf("ReadTransactionInfoStrict corrupt readable block body = info %+v ok %v err %v, want decode error", info, ok, err)
	}
}

func TestReadTransactionInfoUsesReadableBlockPositionWhenInfoIDMissing(t *testing.T) {
	db := NewMemoryChainDB()
	txPB1 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7051, Expiration: 8051, Data: []byte{0x51}}}
	txPB2 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7052, Expiration: 8052, Data: []byte{0x52}}}
	txHash2 := types.NewTransactionFromPB(txPB2).Hash()
	blockPB := newBlockProto(7, 7050)
	blockPB.Transactions = []*corepb.Transaction{txPB1, txPB2}
	block := types.NewBlockFromPB(blockPB)
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionIndex(db, txHash2[:], 7); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Fee: 100, BlockNumber: 7, BlockTimeStamp: 7050},
		{Fee: 200, BlockNumber: 7, BlockTimeStamp: 7050},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}

	got, ok, err := ReadTransactionInfoStrict(db, txHash2[:])
	if err != nil || !ok || got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfoStrict via readable block = %+v ok %v err %v, want fee 200", got, ok, err)
	}
	if got := ReadTransactionInfo(db, txHash2[:]); got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfo via readable block = %+v, want fee 200", got)
	}
}

func TestReadTransactionInfoHotReadablePositionIgnoresColdPositionError(t *testing.T) {
	db := NewMemoryChainDB()
	txPB1 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7061, Expiration: 8061, Data: []byte{0x61}}}
	txPB2 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7062, Expiration: 8062, Data: []byte{0x62}}}
	txHash2 := types.NewTransactionFromPB(txPB2).Hash()
	blockPB := newBlockProto(7, 7060)
	blockPB.Transactions = []*corepb.Transaction{txPB1, txPB2}
	block := types.NewBlockFromPB(blockPB)
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionIndex(db, txHash2[:], 7); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Fee: 100, BlockNumber: 7, BlockTimeStamp: 7060},
		{Fee: 200, BlockNumber: 7, BlockTimeStamp: 7060},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	db.SetChainIndexReader(failingTxPositionChainIndex{
		tx:     txHash2,
		block:  7,
		posErr: errors.New("cold tx position corrupt"),
	})

	got, ok, err := ReadTransactionInfoStrict(db, txHash2[:])
	if err != nil || !ok || got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfoStrict hot readable = %+v ok %v err %v, want fee 200", got, ok, err)
	}
	if got := ReadTransactionInfo(db, txHash2[:]); got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfo hot readable = %+v, want fee 200", got)
	}
}

func TestReadTransactionInfoHotExplicitIDIgnoresColdPositionError(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0x63}, common.HashLength)
	otherID := bytes.Repeat([]byte{0x64}, common.HashLength)
	if err := WriteTransactionIndex(db, txID, 7); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Id: otherID, Fee: 100, BlockNumber: 7, BlockTimeStamp: 7070},
		{Id: txID, Fee: 200, BlockNumber: 7, BlockTimeStamp: 7070},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	var hash common.Hash
	copy(hash[:], txID)
	db.SetChainIndexReader(failingTxPositionChainIndex{
		tx:     hash,
		block:  7,
		posErr: errors.New("cold tx position corrupt"),
	})

	got, ok, err := ReadTransactionInfoStrict(db, txID)
	if err != nil || !ok || got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfoStrict hot explicit id = %+v ok %v err %v, want fee 200", got, ok, err)
	}
	if got := ReadTransactionInfo(db, txID); got == nil || got.Fee != 200 {
		t.Fatalf("ReadTransactionInfo hot explicit id = %+v, want fee 200", got)
	}
}

func TestReadTransactionInfoReadableBlockPositionRejectsCorruptBlockBody(t *testing.T) {
	ancient := newFakeAncient()
	ancient.put(ancientBlocks, 7, []byte("not-a-valid-block"))
	db := NewChainDB(NewMemoryDatabase(), ancient)
	txID := bytes.Repeat([]byte{0x53}, common.HashLength)
	if err := WriteTransactionIndex(db, txID, 7); err != nil {
		t.Fatalf("WriteTransactionIndex: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Id: txID, Fee: 200, BlockNumber: 7, BlockTimeStamp: 7050},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}

	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo corrupt readable block fallback = %+v, want nil", got)
	}
	if got, ok, err := ReadTransactionInfoStrict(db, txID); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "block 7 decode") {
		t.Fatalf("ReadTransactionInfoStrict corrupt readable block fallback = %+v ok %v err %v, want decode error", got, ok, err)
	}
}

func TestReadTransactionInfoRejectsColdTxPositionThatMismatchesReadableBlockBody(t *testing.T) {
	db := NewMemoryChainDB()
	txPB1 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7101, Expiration: 8101, Data: []byte{0x11}}}
	txPB2 := &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 7102, Expiration: 8102, Data: []byte{0x12}}}
	txHash1 := types.NewTransactionFromPB(txPB1).Hash()
	blockPB := newBlockProto(7, 7100)
	blockPB.Transactions = []*corepb.Transaction{txPB1, txPB2}
	block := types.NewBlockFromPB(blockPB)
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Fee: 100, BlockNumber: 7, BlockTimeStamp: 7100},
		{Fee: 200, BlockNumber: 7, BlockTimeStamp: 7100},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	db.SetChainIndexReader(&fakeChainIndex{
		txs: map[common.Hash]uint64{txHash1: 7},
		positions: map[common.Hash]ChainIndexTxLookup{
			txHash1: {BlockNum: 7, TxIndex: 1},
		},
	})

	if got := ReadTransactionInfo(db, txHash1[:]); got != nil {
		t.Fatalf("ReadTransactionInfo accepted cold tx position mismatching block body = %+v, want nil", got)
	}
	if info, ok, err := ReadTransactionInfoStrict(db, txHash1[:]); err == nil || !ok || info != nil {
		t.Fatalf("ReadTransactionInfoStrict mismatching block body = info %+v ok %v err %v, want nil/ok/error", info, ok, err)
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
	if info, ok, err := ReadTransactionInfoStrict(db, txID); err == nil || !ok || info == nil {
		t.Fatalf("ReadTransactionInfoStrict mismatched cold position = info %+v ok %v err %v, want info/ok/error", info, ok, err)
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
	if info, ok, err := ReadTransactionInfoStrict(db, txID); err == nil || !ok || info == nil {
		t.Fatalf("ReadTransactionInfoStrict scanned past mismatched cold position = info %+v ok %v err %v, want info/ok/error", info, ok, err)
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
	if info, ok, err := ReadTransactionInfoStrict(db, txID); err == nil || !ok || info != nil {
		t.Fatalf("ReadTransactionInfoStrict cold block mismatch by position = info %+v ok %v err %v, want nil/ok/error", info, ok, err)
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
	if info, ok, err := ReadTransactionInfoStrict(db, txID2); err == nil || !ok || info != nil {
		t.Fatalf("ReadTransactionInfoStrict cold block mismatch by scan = info %+v ok %v err %v, want nil/ok/error", info, ok, err)
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

func TestReadTransactionInfosByBlockStrictReportsMalformedID(t *testing.T) {
	db := NewMemoryChainDB()
	writeRawTransactionRetForTest(t, db, 5, &corepb.TransactionRet{
		BlockNumber: 5,
		Transactioninfo: []*corepb.TransactionInfo{
			{Id: []byte{0x06}, Fee: 600, BlockNumber: 5},
		},
	})

	if got := ReadTransactionInfosByBlock(db, 5); got != nil {
		t.Fatalf("malformed TransactionInfo id read = %+v, want nil", got)
	}
	infos, ok, err := ReadTransactionInfosByBlockStrict(db, 5)
	if err == nil || !strings.Contains(err.Error(), "transaction info id length 1") {
		t.Fatalf("strict tx-info read = %+v/%v/%v, want id length error", infos, ok, err)
	}
	if !ok {
		t.Fatal("strict tx-info read reported missing source row for malformed id")
	}
}

func TestReadTransactionInfosByBlockStrictSurfacesHotReadError(t *testing.T) {
	base := NewMemoryDatabase()
	db := NewChainDB(base, NoopAncient{})
	txID := bytes.Repeat([]byte{0xd2}, common.HashLength)
	if err := WriteTransactionInfosByBlock(db, 5, []*corepb.TransactionInfo{{Id: txID, Fee: 700, BlockNumber: 5}}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	wantErr := errors.New("hot tx infos unreadable")
	failing := NewChainDB(failingTxInfoHotStore{KeyValueStore: base, getKey: txInfoBlockKey(5), getErr: wantErr}, NoopAncient{})

	if got := ReadTransactionInfosByBlock(failing, 5); got != nil {
		t.Fatalf("ReadTransactionInfosByBlock hot read error = %+v, want nil compatibility miss", got)
	}
	infos, ok, err := ReadTransactionInfosByBlockStrict(failing, 5)
	if !errors.Is(err, wantErr) || ok || infos != nil {
		t.Fatalf("ReadTransactionInfosByBlockStrict hot read error = %+v/%v/%v, want hot error", infos, ok, err)
	}
}

func TestReadTransactionInfosByBlockStrictReportsMissingSource(t *testing.T) {
	db := NewMemoryChainDB()
	infos, ok, err := ReadTransactionInfosByBlockStrict(db, 5)
	if err != nil || ok || infos != nil {
		t.Fatalf("strict missing tx-info read = %+v/%v/%v, want nil/false/nil", infos, ok, err)
	}
}

func TestReadTransactionInfosByBlockStrictSurfacesAncientError(t *testing.T) {
	wantErr := errors.New("ancient tx infos corrupt")
	db := NewChainDB(NewMemoryDatabase(), failingAncientReader{kind: ancientTxInfos, err: wantErr})
	if err := WriteTransactionInfosByBlock(db, 5, []*corepb.TransactionInfo{
		{Id: bytes.Repeat([]byte{0x07}, common.HashLength), Fee: 700, BlockNumber: 5},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}

	if got := ReadTransactionInfosByBlock(db, 5); got != nil {
		t.Fatalf("ReadTransactionInfosByBlock ancient error = %+v, want nil compatibility miss", got)
	}
	infos, ok, err := ReadTransactionInfosByBlockStrict(db, 5)
	if !errors.Is(err, wantErr) || ok || infos != nil {
		t.Fatalf("ReadTransactionInfosByBlockStrict ancient error = %+v/%v/%v, want ancient error", infos, ok, err)
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

func TestWriteTransactionInfosByBlockRejectsMalformedID(t *testing.T) {
	db := NewMemoryChainDB()
	err := WriteTransactionInfosByBlock(db, 5, []*corepb.TransactionInfo{{Id: []byte{0x01}, BlockNumber: 5}})
	if err == nil || !strings.Contains(err.Error(), "transaction info id length 1") {
		t.Fatalf("WriteTransactionInfosByBlock err = %v, want id length error", err)
	}
	if got := ReadTransactionInfosByBlock(db, 5); got != nil {
		t.Fatalf("ReadTransactionInfosByBlock after rejected malformed id = %+v, want nil", got)
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
	strict, ok, err := ReadTransactionIndexStrict(db, txHash)
	if err != nil || !ok || strict != 42 {
		t.Fatalf("ReadTransactionIndexStrict = %d/%v/%v, want 42/true/nil", strict, ok, err)
	}
}

func TestReadTransactionIndexStrictReportsMalformedHotRow(t *testing.T) {
	db := NewMemoryChainDB()

	txHash := bytes.Repeat([]byte{0xCD}, 32)
	if err := db.Put(txKey(txHash), []byte{0x01, 0x02}); err != nil {
		t.Fatalf("put malformed tx index: %v", err)
	}

	if got := ReadTransactionIndex(db, txHash); got != nil {
		t.Fatalf("ReadTransactionIndex malformed hot row = %d, want nil compatibility miss", *got)
	}
	num, ok, err := ReadTransactionIndexStrict(db, txHash)
	if err == nil || !ok || num != 0 || !strings.Contains(err.Error(), "has length 2, want 8") {
		t.Fatalf("ReadTransactionIndexStrict malformed hot row = %d/%v/%v, want length error", num, ok, err)
	}
}

func TestReadTransactionIndexStrictSurfacesColdError(t *testing.T) {
	db := NewMemoryChainDB()
	wantErr := errors.New("cold chain index corrupt")
	db.SetChainIndexReader(failingTxChainIndex{err: wantErr})

	txHash := bytes.Repeat([]byte{0xCE}, 32)
	if got := ReadTransactionIndex(db, txHash); got != nil {
		t.Fatalf("ReadTransactionIndex cold error = %d, want nil compatibility miss", *got)
	}
	num, ok, err := ReadTransactionIndexStrict(db, txHash)
	if !errors.Is(err, wantErr) || ok || num != 0 {
		t.Fatalf("ReadTransactionIndexStrict cold error = %d/%v/%v, want cold error", num, ok, err)
	}
}

func TestReadTransactionInfoStrictSurfacesColdTransactionIndexError(t *testing.T) {
	db := NewMemoryChainDB()
	wantErr := errors.New("cold tx lookup corrupt")
	db.SetChainIndexReader(failingTxChainIndex{err: wantErr})

	txHash := bytes.Repeat([]byte{0xCF}, 32)
	if got := ReadTransactionInfo(db, txHash); got != nil {
		t.Fatalf("ReadTransactionInfo cold tx lookup error = %+v, want nil compatibility miss", got)
	}
	info, ok, err := ReadTransactionInfoStrict(db, txHash)
	if !errors.Is(err, wantErr) || ok || info != nil {
		t.Fatalf("ReadTransactionInfoStrict cold tx lookup error = %+v/%v/%v, want cold error", info, ok, err)
	}
}

func TestReadTransactionInfoStrictSurfacesHotReadError(t *testing.T) {
	base := NewMemoryDatabase()
	db := NewChainDB(base, NoopAncient{})
	txID := bytes.Repeat([]byte{0xd1}, common.HashLength)
	if err := WriteTransactionInfo(db, txID, &corepb.TransactionInfo{Id: txID, Fee: 700, BlockNumber: 7}); err != nil {
		t.Fatalf("WriteTransactionInfo: %v", err)
	}
	wantErr := errors.New("hot tx info unreadable")
	failing := NewChainDB(failingTxInfoHotStore{KeyValueStore: base, getKey: txInfoKey(txID), getErr: wantErr}, NoopAncient{})

	if got := ReadTransactionInfo(failing, txID); got != nil {
		t.Fatalf("ReadTransactionInfo hot read error = %+v, want nil compatibility miss", got)
	}
	info, ok, err := ReadTransactionInfoStrict(failing, txID)
	if !errors.Is(err, wantErr) || ok || info != nil {
		t.Fatalf("ReadTransactionInfoStrict hot read error = %+v/%v/%v, want hot error", info, ok, err)
	}
}

func TestReadTransactionInfoStrictSurfacesColdTransactionPositionError(t *testing.T) {
	db := NewMemoryChainDB()
	txID := bytes.Repeat([]byte{0xd0}, common.HashLength)
	if err := WriteTransactionInfosByBlock(db, 7, []*corepb.TransactionInfo{
		{Id: txID, Fee: 700, BlockNumber: 7, BlockTimeStamp: 7000},
	}); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	wantErr := errors.New("cold tx position corrupt")
	var hash common.Hash
	copy(hash[:], txID)
	db.SetChainIndexReader(failingTxPositionChainIndex{
		tx:     hash,
		block:  7,
		posErr: wantErr,
	})

	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("ReadTransactionInfo cold tx position error = %+v, want nil compatibility miss", got)
	}
	info, ok, err := ReadTransactionInfoStrict(db, txID)
	if !errors.Is(err, wantErr) || ok || info != nil {
		t.Fatalf("ReadTransactionInfoStrict cold tx position error = %+v/%v/%v, want cold position error", info, ok, err)
	}
}

func TestTransactionIndexRejectsMalformedHash(t *testing.T) {
	db := NewMemoryChainDB()
	txHash := []byte{0xcc}
	if err := WriteTransactionIndex(db, txHash, 42); err == nil || !strings.Contains(err.Error(), "transaction hash length 1") {
		t.Fatalf("WriteTransactionIndex malformed hash err = %v, want hash length error", err)
	}
	if _, err := db.Get(txKey(txHash)); err == nil {
		t.Fatal("WriteTransactionIndex malformed hash created a raw row")
	}
	if got := ReadTransactionIndex(db, txHash); got != nil {
		t.Fatalf("ReadTransactionIndex malformed hash = %d, want nil", *got)
	}
	if _, ok, err := ReadTransactionIndexStrict(db, txHash); err == nil || ok || !strings.Contains(err.Error(), "transaction hash length 1") {
		t.Fatalf("ReadTransactionIndexStrict malformed hash ok=%v err=%v, want hash length error", ok, err)
	}
	if err := DeleteTransactionIndex(db, txHash); err == nil || !strings.Contains(err.Error(), "transaction hash length 1") {
		t.Fatalf("DeleteTransactionIndex malformed hash err = %v, want hash length error", err)
	}
	if err := DeleteTransactionInfo(db, txHash); err == nil || !strings.Contains(err.Error(), "transaction hash length 1") {
		t.Fatalf("DeleteTransactionInfo malformed hash err = %v, want hash length error", err)
	}
}

func TestReadTransactionIndex_NotFound(t *testing.T) {
	db := NewMemoryChainDB()
	got := ReadTransactionIndex(db, bytes.Repeat([]byte{0x00}, 32))
	if got != nil {
		t.Fatal("expected nil for missing tx index")
	}
	if num, ok, err := ReadTransactionIndexStrict(db, bytes.Repeat([]byte{0x00}, 32)); err != nil || ok || num != 0 {
		t.Fatalf("ReadTransactionIndexStrict missing = %d/%v/%v, want 0/false/nil", num, ok, err)
	}
}

type failingTxChainIndex struct {
	err error
}

func (f failingTxChainIndex) BlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	return 0, false, nil
}

func (f failingTxChainIndex) TransactionBlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	return 0, false, f.err
}

type failingTxPositionChainIndex struct {
	tx     common.Hash
	block  uint64
	posErr error
}

func (f failingTxPositionChainIndex) BlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	return 0, false, nil
}

func (f failingTxPositionChainIndex) TransactionBlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	if hash == f.tx {
		return f.block, true, nil
	}
	return 0, false, nil
}

func (f failingTxPositionChainIndex) TransactionIndexByHash(hash common.Hash) (ChainIndexTxLookup, bool, error) {
	if hash == f.tx {
		return ChainIndexTxLookup{}, false, f.posErr
	}
	return ChainIndexTxLookup{}, false, nil
}

type failingTxInfoHotStore struct {
	ethdb.KeyValueStore
	getKey []byte
	getErr error
	hasErr error
}

func (s failingTxInfoHotStore) Has(key []byte) (bool, error) {
	if bytes.Equal(key, s.getKey) && s.hasErr != nil {
		return false, s.hasErr
	}
	return s.KeyValueStore.Has(key)
}

func (s failingTxInfoHotStore) Get(key []byte) ([]byte, error) {
	if bytes.Equal(key, s.getKey) && s.getErr != nil {
		return nil, s.getErr
	}
	return s.KeyValueStore.Get(key)
}

type failingAncientReader struct {
	kind string
	err  error
}

func (f failingAncientReader) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == f.kind {
		return nil, f.err
	}
	return nil, ErrNotInAncient
}

func (f failingAncientReader) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if kind == f.kind {
		return nil, f.err
	}
	return nil, ErrNotInAncient
}

func (f failingAncientReader) AncientCount(kind string) (uint64, error) {
	if kind == f.kind {
		return 0, f.err
	}
	return 0, nil
}

func (f failingAncientReader) HasAncient(kind string, number uint64) (bool, error) {
	if kind == f.kind {
		return false, f.err
	}
	return false, nil
}
