package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func TestRebuildTransactionDerivedIndexesFromBlocks(t *testing.T) {
	db := NewMemoryChainDB()
	block1, infos1 := derivedRebuildTestBlock(t, 1, 2)
	block2, infos2 := derivedRebuildTestBlock(t, 2, 1)
	block3, infos3 := derivedRebuildTestBlock(t, 3, 1)
	if err := WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := WriteBlock(db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	if err := WriteBlock(db, block3); err != nil {
		t.Fatalf("WriteBlock block3: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block1: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 2, infos2); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock block2: %v", err)
	}
	// Leave block 3 without TransactionRet coverage. The rebuild must still
	// terminate and rebuild its tx lookup row from the block body.
	txID := infos1[1].Id
	if got := ReadTransactionIndex(db, txID); got != nil {
		t.Fatalf("pre-rebuild tx index = %v, want nil", got)
	}
	if got := ReadTransactionInfo(db, txID); got != nil {
		t.Fatalf("pre-rebuild tx info = %+v, want nil", got)
	}

	result, err := RebuildTransactionDerivedIndexesFromBlocks(db, db, 1, 3, etl.Options{
		TempDir:     t.TempDir(),
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("RebuildTransactionDerivedIndexesFromBlocks: %v", err)
	}
	if result.BlocksScanned != 3 || result.TransactionsIndexed != 4 ||
		result.BlocksWithTxInfo != 2 || result.TransactionInfosIndexed != 3 {
		t.Fatalf("result = %+v, want 3 blocks, 4 txs, 2 info blocks, 3 infos", result)
	}
	if result.ETL.SpilledRuns == 0 {
		t.Fatalf("ETL spilled runs = %d, want forced spill", result.ETL.SpilledRuns)
	}
	if got := ReadTransactionIndex(db, txID); got == nil || *got != 1 {
		t.Fatalf("post-rebuild tx index = %v, want 1", got)
	}
	if got := ReadTransactionInfo(db, txID); got == nil || got.Fee != infos1[1].Fee {
		t.Fatalf("post-rebuild tx info = %+v, want fee %d", got, infos1[1].Fee)
	}
	if got := ReadTransactionInfosByBlock(db, 2); len(got) != 1 || got[0].Fee != infos2[0].Fee {
		t.Fatalf("post-rebuild infos by block2 = %+v, want one fee %d", got, infos2[0].Fee)
	}
	if got := ReadTransactionIndex(db, infos3[0].Id); got == nil || *got != 3 {
		t.Fatalf("post-rebuild tx index for block3 = %v, want 3", got)
	}
	if got := ReadTransactionInfo(db, infos3[0].Id); got != nil {
		t.Fatalf("post-rebuild tx info for block3 = %+v, want nil without TransactionRet", got)
	}
}

func TestRebuildTransactionDerivedIndexesFromAncientTxInfos(t *testing.T) {
	hot := NewMemoryDatabase()
	anc := newFakeAncient()
	block, infos := derivedRebuildTestBlock(t, 7, 1)
	blockRaw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     7,
		BlockTimeStamp:  infos[0].BlockTimeStamp,
		Transactioninfo: infos,
	}
	retRaw, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal tx ret: %v", err)
	}
	anc.put(ancientBlocks, 7, blockRaw)
	anc.put(ancientTxInfos, 7, retRaw)
	db := NewChainDB(hot, anc)

	result, err := RebuildTransactionDerivedIndexesFromBlocks(db, db, 7, 7, etl.Options{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("RebuildTransactionDerivedIndexesFromBlocks ancient: %v", err)
	}
	if result.BlocksScanned != 1 || result.TransactionsIndexed != 1 || result.TransactionInfosIndexed != 1 {
		t.Fatalf("result = %+v, want one rebuilt tx/info", result)
	}
	txID := infos[0].Id
	if got := ReadTransactionIndex(db, txID); got == nil || *got != 7 {
		t.Fatalf("post-ancient-rebuild tx index = %v, want 7", got)
	}
	if got := ReadTransactionInfo(db, txID); got == nil || got.Fee != infos[0].Fee {
		t.Fatalf("post-ancient-rebuild tx info = %+v, want fee %d", got, infos[0].Fee)
	}
	if got := ReadTransactionInfosByBlock(db, 7); len(got) != 1 || got[0].Fee != infos[0].Fee {
		t.Fatalf("post-ancient-rebuild infos by block = %+v, want one fee %d", got, infos[0].Fee)
	}
}

func TestRebuildTransactionDerivedIndexesRejectsBadInputs(t *testing.T) {
	db := NewMemoryChainDB()
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(nil, db, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil chain accepted")
	}
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(db, nil, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil writer accepted")
	}
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(db, db, 2, 1, etl.Options{}); err == nil || !strings.Contains(err.Error(), "inverted") {
		t.Fatalf("inverted range err = %v, want inverted", err)
	}
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(db, db, 9, 9, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "missing block 9") {
		t.Fatalf("missing block err = %v, want missing block 9", err)
	}
	if err := db.Put(blockKey(10), []byte("not-a-valid-block")); err != nil {
		t.Fatalf("put malformed block: %v", err)
	}
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(db, db, 10, 10, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "block 10 decode") {
		t.Fatalf("malformed block err = %v, want block decode error", err)
	}
}

func TestRebuildTransactionDerivedIndexesRejectsMismatchedTransactionInfo(t *testing.T) {
	mismatchedDB := NewMemoryChainDB()
	block, infos := derivedRebuildTestBlock(t, 1, 1)
	infos[0].Id = bytes.Repeat([]byte{0xef}, common.HashLength)
	if err := WriteBlock(mismatchedDB, block); err != nil {
		t.Fatalf("WriteBlock mismatchedDB: %v", err)
	}
	if err := WriteTransactionInfosByBlock(mismatchedDB, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock mismatchedDB: %v", err)
	}
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(mismatchedDB, mismatchedDB, 1, 1, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("mismatched tx-info id err = %v, want canonical tx mismatch", err)
	}

	wrongBlockDB := NewMemoryChainDB()
	block, infos = derivedRebuildTestBlock(t, 1, 1)
	infos[0].BlockNumber = 2
	if err := WriteBlock(wrongBlockDB, block); err != nil {
		t.Fatalf("WriteBlock wrongBlockDB: %v", err)
	}
	writeRawTransactionRetForTest(t, wrongBlockDB, 1, &corepb.TransactionRet{
		BlockNumber:     1,
		Transactioninfo: infos,
	})
	if _, err := RebuildTransactionDerivedIndexesFromBlocks(wrongBlockDB, wrongBlockDB, 1, 1, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "transaction info block number") {
		t.Fatalf("wrong block tx-info err = %v, want block number mismatch", err)
	}
}

func TestRebuildSectionBloomsFromTransactionInfos(t *testing.T) {
	db := NewMemoryChainDB()
	block, infos := derivedRebuildTestBlock(t, 1, 1)
	infos[0].Log = []*corepb.TransactionInfo_Log{{
		Address: []byte{0x11, 0x22, 0x33, 0x44, 0x55},
		Topics: [][]byte{
			{0xaa, 0xbb, 0xcc},
			{0x01, 0x02, 0x03, 0x04},
		},
	}}
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	bitIndexes, _, _ := sectionBloomBitsFromTransactionInfos(infos)
	if len(bitIndexes) == 0 {
		t.Fatal("test fixture produced no section bloom bits")
	}

	result, err := RebuildSectionBloomsFromTransactionInfos(db, db, db, 1, 1, etl.Options{
		TempDir:     t.TempDir(),
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("RebuildSectionBloomsFromTransactionInfos: %v", err)
	}
	if result.BlocksScanned != 1 || result.BlocksWithTransactionInfos != 1 ||
		result.BlocksWithLogs != 1 || result.LogEntriesIndexed != 1 ||
		result.BloomItemsIndexed != 3 || result.SectionBloomRows != uint64(len(bitIndexes)) {
		t.Fatalf("result = %+v, want one block/log and %d rows", result, len(bitIndexes))
	}
	if result.ETL.SpilledRuns == 0 {
		t.Fatalf("ETL spilled runs = %d, want forced spill", result.ETL.SpilledRuns)
	}
	for _, bitIndex := range bitIndexes {
		bitset, ok, err := ReadSectionBloomBitSet(db, 0, bitIndex)
		if err != nil {
			t.Fatalf("ReadSectionBloomBitSet %d: %v", bitIndex, err)
		}
		if !ok {
			t.Fatalf("missing section bloom row for bit %d", bitIndex)
		}
		if !sectionBloomBitSetHas(bitset, 1) {
			t.Fatalf("section bloom bit %d does not include block offset 1: %x", bitIndex, bitset)
		}
	}
}

func TestRebuildSectionBloomsPreservesExistingSectionBits(t *testing.T) {
	db := NewMemoryChainDB()
	block, infos := derivedRebuildTestBlock(t, 1, 1)
	infos[0].Log = []*corepb.TransactionInfo_Log{{
		Address: []byte{0x99, 0x88, 0x77},
	}}
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	bitIndexes, _, _ := sectionBloomBitsFromTransactionInfos(infos)
	preservedBit := bitIndexes[0]
	preserved := setSectionBloomBit(nil, 12)
	encoded, err := EncodeSectionBloomBitSet(preserved)
	if err != nil {
		t.Fatalf("EncodeSectionBloomBitSet: %v", err)
	}
	if err := WriteSectionBloom(db, 0, preservedBit, encoded); err != nil {
		t.Fatalf("WriteSectionBloom preserved row: %v", err)
	}

	if _, err := RebuildSectionBloomsFromTransactionInfos(db, db, db, 1, 1, etl.Options{TempDir: t.TempDir()}); err != nil {
		t.Fatalf("RebuildSectionBloomsFromTransactionInfos: %v", err)
	}
	got, ok, err := ReadSectionBloomBitSet(db, 0, preservedBit)
	if err != nil {
		t.Fatalf("ReadSectionBloomBitSet: %v", err)
	}
	if !ok {
		t.Fatal("preserved row missing after rebuild")
	}
	if !sectionBloomBitSetHas(got, 12) {
		t.Fatalf("existing block offset 12 was cleared: %x", got)
	}
	if !sectionBloomBitSetHas(got, 1) {
		t.Fatalf("new block offset 1 was not added: %x", got)
	}
}

func TestRebuildSectionBloomsRejectsColdSectionBloomReadError(t *testing.T) {
	db := NewMemoryChainDB()
	block, infos := derivedRebuildTestBlock(t, 1, 1)
	infos[0].Log = []*corepb.TransactionInfo_Log{{
		Address: []byte{0x99, 0x88, 0x77},
	}}
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	sectionReader := NewMemoryChainDB()
	sectionReader.SetSectionBloomReader(fakeSectionBloomReader{err: errors.New("cold section bloom unavailable")})

	_, err := RebuildSectionBloomsFromTransactionInfos(db, sectionReader, db, 1, 1, etl.Options{TempDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "cold section bloom unavailable") {
		t.Fatalf("RebuildSectionBloomsFromTransactionInfos cold reader error = %v, want cold section bloom error", err)
	}
	bitIndexes, _, _ := sectionBloomBitsFromTransactionInfos(infos)
	if len(bitIndexes) == 0 {
		t.Fatal("test fixture produced no section bloom bits")
	}
	if got := ReadSectionBloom(db, 0, bitIndexes[0]); got != nil {
		t.Fatalf("section bloom row written despite cold read error = %x", got)
	}
}

func TestRebuildSectionBloomsRejectsBadInputs(t *testing.T) {
	db := NewMemoryChainDB()
	if _, err := RebuildSectionBloomsFromTransactionInfos(nil, db, db, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil chain accepted")
	}
	if _, err := RebuildSectionBloomsFromTransactionInfos(db, nil, db, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil reader accepted")
	}
	if _, err := RebuildSectionBloomsFromTransactionInfos(db, db, nil, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil writer accepted")
	}
	if _, err := RebuildSectionBloomsFromTransactionInfos(db, db, db, 2, 1, etl.Options{}); err == nil || !strings.Contains(err.Error(), "inverted") {
		t.Fatalf("inverted range err = %v, want inverted", err)
	}
	if _, err := RebuildSectionBloomsFromTransactionInfos(db, db, db, 9, 9, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "missing block 9") {
		t.Fatalf("missing block err = %v, want missing block 9", err)
	}
	missingInfoDB := NewMemoryChainDB()
	block, _ := derivedRebuildTestBlock(t, 1, 1)
	if err := WriteBlock(missingInfoDB, block); err != nil {
		t.Fatalf("WriteBlock missingInfoDB: %v", err)
	}
	if _, err := RebuildSectionBloomsFromTransactionInfos(missingInfoDB, missingInfoDB, missingInfoDB, 1, 1, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "incomplete transaction info coverage") {
		t.Fatalf("missing tx-info coverage err = %v, want incomplete transaction info coverage", err)
	}
	if err := ValidateTransactionInfosForBlock(1, block.Transactions(), []*corepb.TransactionInfo{nil}, "section bloom rebuild"); err == nil || !strings.Contains(err.Error(), "nil transaction info") {
		t.Fatalf("nil tx-info err = %v, want nil transaction info", err)
	}
	mismatchedDB := NewMemoryChainDB()
	block, infos := derivedRebuildTestBlock(t, 1, 1)
	infos[0].Id = bytes.Repeat([]byte{0xfe}, common.HashLength)
	if err := WriteBlock(mismatchedDB, block); err != nil {
		t.Fatalf("WriteBlock mismatchedDB: %v", err)
	}
	if err := WriteTransactionInfosByBlock(mismatchedDB, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock mismatchedDB: %v", err)
	}
	if _, err := RebuildSectionBloomsFromTransactionInfos(mismatchedDB, mismatchedDB, mismatchedDB, 1, 1, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "does not match canonical tx") {
		t.Fatalf("mismatched tx-info id err = %v, want canonical tx mismatch", err)
	}
	wrongBlockDB := NewMemoryChainDB()
	block, infos = derivedRebuildTestBlock(t, 1, 1)
	infos[0].BlockNumber = 2
	if err := WriteBlock(wrongBlockDB, block); err != nil {
		t.Fatalf("WriteBlock wrongBlockDB: %v", err)
	}
	writeRawTransactionRetForTest(t, wrongBlockDB, 1, &corepb.TransactionRet{
		BlockNumber:     1,
		Transactioninfo: infos,
	})
	if _, err := RebuildSectionBloomsFromTransactionInfos(wrongBlockDB, wrongBlockDB, wrongBlockDB, 1, 1, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "transaction info block number") {
		t.Fatalf("wrong block tx-info err = %v, want block number mismatch", err)
	}
}

func TestRebuildAccountTracesFromBlockBalanceTraces(t *testing.T) {
	db := NewMemoryChainDB()
	block1, infos1 := derivedRebuildTestBlock(t, 1, 2)
	block2, infos2 := derivedRebuildTestBlock(t, 2, 1)
	if err := WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock block1: %v", err)
	}
	if err := WriteBlock(db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	a := derivedRebuildAddress(0xa0)
	b := derivedRebuildAddress(0xb0)
	c := derivedRebuildAddress(0xc0)
	if err := WriteBlockBalanceTrace(db, 1, derivedRebuildBalanceTrace(block1, infos1,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, a, 100),
			derivedRebuildBalanceOp(1, b, 200),
		},
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, a, -30),
		},
	)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block1: %v", err)
	}
	if err := WriteBlockBalanceTrace(db, 2, derivedRebuildBalanceTrace(block2, infos2,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, a, 5),
			derivedRebuildBalanceOp(1, b, -50),
			derivedRebuildBalanceOp(2, c, 7),
		},
	)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block2: %v", err)
	}

	result, err := RebuildAccountTracesFromBlockBalanceTraces(db, db, db, 1, 2, etl.Options{
		TempDir:     t.TempDir(),
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("RebuildAccountTracesFromBlockBalanceTraces: %v", err)
	}
	if result.BlocksScanned != 2 || result.BlocksWithBalanceTrace != 2 ||
		result.TransactionsScanned != 3 || result.OperationsApplied != 6 ||
		result.AccountTraceRows != 5 {
		t.Fatalf("result = %+v, want 2 blocks, 3 txs, 6 ops, 5 account rows", result)
	}
	if result.ETL.SpilledRuns == 0 {
		t.Fatalf("ETL spilled runs = %d, want forced spill", result.ETL.SpilledRuns)
	}
	for _, tc := range []struct {
		addr  []byte
		block int64
		want  int64
	}{
		{a, 1, 70},
		{b, 1, 200},
		{a, 2, 75},
		{b, 2, 150},
		{c, 2, 7},
	} {
		got, ok := ReadAccountTrace(db, tc.addr, tc.block)
		if !ok || got != tc.want {
			t.Fatalf("ReadAccountTrace addr=%x block=%d = %d/%v, want %d/true", tc.addr, tc.block, got, ok, tc.want)
		}
	}
}

func TestRebuildAccountTracesUsesExistingBaselineForPartialRange(t *testing.T) {
	db := NewMemoryChainDB()
	block2, infos2 := derivedRebuildTestBlock(t, 2, 1)
	if err := WriteBlock(db, block2); err != nil {
		t.Fatalf("WriteBlock block2: %v", err)
	}
	a := derivedRebuildAddress(0xa1)
	if err := WriteAccountTrace(db, a, 1, 70); err != nil {
		t.Fatalf("WriteAccountTrace baseline: %v", err)
	}
	if err := WriteBlockBalanceTrace(db, 2, derivedRebuildBalanceTrace(block2, infos2,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, a, 5),
		},
	)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block2: %v", err)
	}

	result, err := RebuildAccountTracesFromBlockBalanceTraces(db, db, db, 2, 2, etl.Options{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("RebuildAccountTracesFromBlockBalanceTraces partial: %v", err)
	}
	if result.AccountTraceRows != 1 {
		t.Fatalf("AccountTraceRows = %d, want 1", result.AccountTraceRows)
	}
	got, ok := ReadAccountTrace(db, a, 2)
	if !ok || got != 75 {
		t.Fatalf("ReadAccountTrace partial = %d/%v, want 75/true", got, ok)
	}
}

func TestAuditBlockBalanceTraceCoverage(t *testing.T) {
	db := NewMemoryChainDB()
	block1, infos1 := derivedRebuildTestBlock(t, 1, 1)
	block2, _ := derivedRebuildTestBlock(t, 2, 0)
	block3, infos3 := derivedRebuildTestBlock(t, 3, 1)
	for _, block := range []*types.Block{block1, block2, block3} {
		if err := WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", block.Number(), err)
		}
	}
	addr := derivedRebuildAddress(0xa2)
	if err := WriteBlockBalanceTrace(db, 1, derivedRebuildBalanceTrace(block1, infos1,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, addr, 1),
		},
	)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace block1: %v", err)
	}
	if err := WriteAccountTrace(db, addr, 1, 1); err != nil {
		t.Fatalf("WriteAccountTrace block1: %v", err)
	}
	writeRawBlockBalanceTraceForRebuildTest(t, db, 3, derivedRebuildBalanceTrace(block1, infos3,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, addr, 2),
		},
	))

	result, err := AuditBlockBalanceTraceCoverage(db, db, 1, 3, 8)
	if err != nil {
		t.Fatalf("AuditBlockBalanceTraceCoverage: %v", err)
	}
	if result.Complete() {
		t.Fatal("coverage unexpectedly complete")
	}
	if result.BlocksScanned != 3 || result.BlocksWithBalanceTrace != 2 ||
		result.MissingBlockBalanceTrace != 1 || result.MissingAccountTrace != 0 || result.MismatchedBlockBalanceTrace != 1 {
		t.Fatalf("result = %+v, want scanned=3 trace=2 missingBlock=1 missingAccount=0 mismatched=1", result)
	}
	if len(result.Issues) != 2 {
		t.Fatalf("issues = %+v, want 2 examples", result.Issues)
	}
	if result.Issues[0].BlockNum != 2 || result.Issues[0].Kind != "missing" {
		t.Fatalf("issue[0] = %+v, want missing block 2", result.Issues[0])
	}
	if result.Issues[1].BlockNum != 3 || result.Issues[1].Kind != "mismatch" {
		t.Fatalf("issue[1] = %+v, want mismatch block 3", result.Issues[1])
	}
}

func TestAuditBlockBalanceTraceCoverageRequiresAccountTraceRows(t *testing.T) {
	db := NewMemoryChainDB()
	block, infos := derivedRebuildTestBlock(t, 1, 1)
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	addr := derivedRebuildAddress(0xa3)
	if err := WriteBlockBalanceTrace(db, 1, derivedRebuildBalanceTrace(block, infos,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, addr, 1),
		},
	)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}

	result, err := AuditBlockBalanceTraceCoverage(db, db, 1, 1, 8)
	if err != nil {
		t.Fatalf("AuditBlockBalanceTraceCoverage: %v", err)
	}
	if result.Complete() {
		t.Fatal("coverage unexpectedly complete")
	}
	if result.MissingBlockBalanceTrace != 0 || result.MissingAccountTrace != 1 || result.MismatchedBlockBalanceTrace != 0 {
		t.Fatalf("result = %+v, want one missing account trace", result)
	}
	if len(result.Issues) != 1 || result.Issues[0].Kind != "missing-account" || result.Issues[0].BlockNum != 1 {
		t.Fatalf("issues = %+v, want one missing-account issue for block 1", result.Issues)
	}
	if err := WriteAccountTrace(db, addr, 1, 1); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	result, err = AuditBlockBalanceTraceCoverage(db, db, 1, 1, 8)
	if err != nil {
		t.Fatalf("second AuditBlockBalanceTraceCoverage: %v", err)
	}
	if !result.Complete() || result.MissingAccountTrace != 0 {
		t.Fatalf("complete result = %+v, want no missing account trace", result)
	}
}

func TestRebuildAccountTracesRejectsBadInputs(t *testing.T) {
	db := NewMemoryChainDB()
	if _, err := RebuildAccountTracesFromBlockBalanceTraces(nil, db, db, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil chain accepted")
	}
	if _, err := RebuildAccountTracesFromBlockBalanceTraces(db, nil, db, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil reader accepted")
	}
	if _, err := RebuildAccountTracesFromBlockBalanceTraces(db, db, nil, 1, 1, etl.Options{}); err == nil {
		t.Fatal("nil writer accepted")
	}
	if _, err := RebuildAccountTracesFromBlockBalanceTraces(db, db, db, 2, 1, etl.Options{}); err == nil || !strings.Contains(err.Error(), "inverted") {
		t.Fatalf("inverted range err = %v, want inverted", err)
	}
	if _, err := RebuildAccountTracesFromBlockBalanceTraces(db, db, db, 9, 9, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "missing block 9") {
		t.Fatalf("missing block err = %v, want missing block 9", err)
	}

	block, infos := derivedRebuildTestBlock(t, 1, 1)
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := WriteBlockBalanceTrace(db, 1, derivedRebuildBalanceTrace(block, infos,
		[]*contractpb.TransactionBalanceTrace_Operation{
			derivedRebuildBalanceOp(0, []byte{0x41}, 1),
		},
	)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace malformed trace: %v", err)
	}
	if _, err := RebuildAccountTracesFromBlockBalanceTraces(db, db, db, 1, 1, etl.Options{TempDir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "malformed balance trace address") {
		t.Fatalf("malformed address err = %v, want malformed address", err)
	}
}

func derivedRebuildTestBlock(t *testing.T, number uint64, txCount int) (*types.Block, []*corepb.TransactionInfo) {
	t.Helper()
	txs := make([]*corepb.Transaction, 0, txCount)
	infos := make([]*corepb.TransactionInfo, 0, txCount)
	for i := 0; i < txCount; i++ {
		txPB := &corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				Timestamp:  int64(10_000 + number*100 + uint64(i)),
				Expiration: int64(20_000 + number*100 + uint64(i)),
				Data:       []byte{byte(number), byte(i)},
			},
		}
		tx := types.NewTransactionFromPB(txPB)
		txHash := tx.Hash()
		txs = append(txs, txPB)
		infos = append(infos, &corepb.TransactionInfo{
			Id:             append([]byte(nil), txHash[:]...),
			Fee:            int64(1_000 + number*10 + uint64(i)),
			BlockNumber:    int64(number),
			BlockTimeStamp: int64(30_000 + number),
		})
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: txs,
	})
	for i, tx := range block.Transactions() {
		if !bytes.Equal(tx.Hash().Bytes(), infos[i].Id) {
			t.Fatalf("tx hash mismatch at %d", i)
		}
	}
	return block, infos
}

func derivedRebuildAddress(seed byte) []byte {
	out := make([]byte, common.AddressLength)
	out[0] = common.AddressPrefixMainnet
	for i := 1; i < len(out); i++ {
		out[i] = seed + byte(i)
	}
	return out
}

func derivedRebuildBalanceOp(id int64, addr []byte, amount int64) *contractpb.TransactionBalanceTrace_Operation {
	return &contractpb.TransactionBalanceTrace_Operation{
		OperationIdentifier: id,
		Address:             append([]byte(nil), addr...),
		Amount:              amount,
	}
}

func derivedRebuildBalanceTrace(block *types.Block, infos []*corepb.TransactionInfo, opSets ...[]*contractpb.TransactionBalanceTrace_Operation) *contractpb.BlockBalanceTrace {
	traces := make([]*contractpb.TransactionBalanceTrace, 0, len(opSets))
	for i, ops := range opSets {
		txID := []byte(nil)
		if i < len(infos) {
			txID = append([]byte(nil), infos[i].Id...)
		}
		traces = append(traces, &contractpb.TransactionBalanceTrace{
			TransactionIdentifier: txID,
			Operation:             ops,
			Type:                  "TransferContract",
			Status:                "SUCCESS",
		})
	}
	return &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), block.Hash().Bytes()...),
			Number: int64(block.Number()),
		},
		Timestamp:               int64(30_000 + block.Number()),
		TransactionBalanceTrace: traces,
	}
}

func writeRawBlockBalanceTraceForRebuildTest(t *testing.T, db *ChainDB, blockNum uint64, trace *contractpb.BlockBalanceTrace) {
	t.Helper()
	data, err := proto.Marshal(trace)
	if err != nil {
		t.Fatalf("marshal raw BlockBalanceTrace: %v", err)
	}
	if err := db.Put(balanceTraceKey(int64(blockNum)), data); err != nil {
		t.Fatalf("put raw BlockBalanceTrace: %v", err)
	}
}

func sectionBloomBitSetHas(bitset []byte, bit uint64) bool {
	byteIndex := bit / 8
	if byteIndex >= uint64(len(bitset)) {
		return false
	}
	return bitset[byteIndex]&(1<<(bit%8)) != 0
}

func BenchmarkRebuildTransactionDerivedIndexes(b *testing.B) {
	source := newTransactionDerivedRebuildBenchSource(b, 128, 2)
	b.Run("direct_unordered", func(b *testing.B) {
		benchmarkTransactionDerivedRebuild(b, false, func(w *derivedIndexRecordingWriter, _ string) error {
			return directRebuildTransactionDerivedIndexes(source, w, 1, 128)
		})
	})
	b.Run("sorted_etl", func(b *testing.B) {
		benchmarkTransactionDerivedRebuild(b, true, func(w *derivedIndexRecordingWriter, tempDir string) error {
			_, err := RebuildTransactionDerivedIndexesFromBlocks(source, w, 1, 128, etl.Options{
				TempDir:     tempDir,
				BufferLimit: 32 << 10,
			})
			return err
		})
	})
}

type transactionDerivedRebuildBenchFunc func(w *derivedIndexRecordingWriter, tempDir string) error

func benchmarkTransactionDerivedRebuild(b *testing.B, expectSorted bool, rebuild transactionDerivedRebuildBenchFunc) {
	b.Helper()
	b.ReportAllocs()
	tempDir := b.TempDir()
	var totalPuts uint64
	var totalOutOfOrder uint64
	for i := 0; i < b.N; i++ {
		sink := newDerivedIndexRecordingWriter()
		if err := rebuild(sink, tempDir); err != nil {
			b.Fatal(err)
		}
		if expectSorted && sink.outOfOrder != 0 {
			b.Fatalf("transaction derived rebuild had %d out-of-order writes, want sorted", sink.outOfOrder)
		}
		if !expectSorted && sink.outOfOrder == 0 {
			b.Fatal("direct transaction derived rebuild produced no out-of-order writes")
		}
		totalPuts += sink.puts
		totalOutOfOrder += sink.outOfOrder
	}
	if totalPuts > 0 {
		b.ReportMetric(float64(totalOutOfOrder)/float64(totalPuts), "out_of_order/put")
	}
}

func newTransactionDerivedRebuildBenchSource(b *testing.B, blocks uint64, txsPerBlock int) *ChainDB {
	b.Helper()
	db := NewMemoryChainDB()
	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		block, infos := derivedRebuildBenchBlock(b, blockNum, txsPerBlock)
		if err := WriteBlock(db, block); err != nil {
			b.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			b.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	return db
}

func derivedRebuildBenchBlock(b *testing.B, number uint64, txCount int) (*types.Block, []*corepb.TransactionInfo) {
	b.Helper()
	txs := make([]*corepb.Transaction, 0, txCount)
	infos := make([]*corepb.TransactionInfo, 0, txCount)
	for i := 0; i < txCount; i++ {
		txPB := &corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				Timestamp:  int64(100_000 + number*100 + uint64(i)),
				Expiration: int64(200_000 + number*100 + uint64(i)),
				Data:       []byte(fmt.Sprintf("bench-tx-%04d-%02d", number, i)),
			},
		}
		tx := types.NewTransactionFromPB(txPB)
		txHash := tx.Hash()
		txs = append(txs, txPB)
		infos = append(infos, &corepb.TransactionInfo{
			Id:             append([]byte(nil), txHash[:]...),
			Fee:            int64(10_000 + number*10 + uint64(i)),
			BlockNumber:    int64(number),
			BlockTimeStamp: int64(300_000 + number),
		})
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(300_000 + number),
			},
		},
		Transactions: txs,
	})
	return block, infos
}

func directRebuildTransactionDerivedIndexes(chain *ChainDB, writer *derivedIndexRecordingWriter, fromBlock, toBlock uint64) error {
	for blockNum := fromBlock; ; blockNum++ {
		block := ReadBlock(chain, blockNum)
		if block == nil {
			return fmt.Errorf("missing block %d", blockNum)
		}
		for _, tx := range block.Transactions() {
			if tx == nil {
				continue
			}
			txHash := tx.Hash()
			if err := WriteTransactionIndex(writer, txHash[:], blockNum); err != nil {
				return err
			}
		}
		infos := ReadTransactionInfosByBlock(chain, blockNum)
		if len(infos) != 0 {
			if err := WriteTransactionInfosByBlock(writer, blockNum, infos); err != nil {
				return err
			}
			for _, info := range infos {
				if info == nil || len(info.Id) == 0 {
					continue
				}
				if err := WriteTransactionInfo(writer, info.Id, info); err != nil {
					return err
				}
			}
		}
		if blockNum == toBlock {
			return nil
		}
	}
}
