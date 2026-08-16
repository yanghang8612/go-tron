package rawdb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestTransactionInfoExternalLogsRoundTripThroughChainDB(t *testing.T) {
	block, txHashes := testChainDBEventLogBlockWithTransactions(7, 2)
	address0 := testChainDBEventLogAddress(0x31)
	address1 := testChainDBEventLogAddress(0x32)
	topic0 := common.Hash{0x41}
	topic1 := common.Hash{0x42}
	log0 := &corepb.TransactionInfo_Log{Address: []byte{0x31}, Topics: [][]byte{topic0[:]}, Data: bytes.Repeat([]byte{0xaa}, 512)}
	log1 := &corepb.TransactionInfo_Log{Address: []byte{0x32}, Topics: [][]byte{topic1[:]}, Data: bytes.Repeat([]byte{0xbb}, 512)}
	infos := []*corepb.TransactionInfo{
		{Id: txHashes[0][:], BlockNumber: 7, Fee: 11, Log: []*corepb.TransactionInfo_Log{log0}},
		{Id: txHashes[1][:], BlockNumber: 7, Fee: 22, Log: []*corepb.TransactionInfo_Log{log1}},
	}
	encoded, err := marshalTransactionInfosByBlock(7, infos, block.Timestamp())
	if err != nil {
		t.Fatal(err)
	}
	external, removed, err := ExternalizeTransactionInfoLogs(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 || len(external) >= len(encoded) {
		t.Fatalf("externalized bytes=%d original=%d removed=%d, want smaller", len(external), len(encoded), removed)
	}

	db := NewMemoryDatabase()
	if err := WriteBlock(db, block); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransactionInfosRaw(db, block.Number(), external); err != nil {
		t.Fatal(err)
	}
	cdb := NewChainDB(db, NoopAncient{})
	cdb.SetEventLogReader(&recordingCoveredEventLogReader{
		covered: true,
		rows: []EventLog{
			{BlockNum: 7, TxIndex: 0, LogIndex: 0, TxHash: txHashes[0], BlockHash: block.Hash(), Address: address0, Log: proto.Clone(log0).(*corepb.TransactionInfo_Log)},
			{BlockNum: 7, TxIndex: 1, LogIndex: 1, TxHash: txHashes[1], BlockHash: block.Hash(), Address: address1, Log: proto.Clone(log1).(*corepb.TransactionInfo_Log)},
		},
	})

	got, ok, err := ReadTransactionInfosByBlockStrict(cdb, 7)
	if err != nil || !ok || len(got) != 2 {
		t.Fatalf("ReadTransactionInfosByBlockStrict = %+v/%v/%v", got, ok, err)
	}
	if got[0].Fee != 11 || got[1].Fee != 22 || len(got[0].Log) != 1 || len(got[1].Log) != 1 || !proto.Equal(got[0].Log[0], log0) || !proto.Equal(got[1].Log[0], log1) {
		t.Fatalf("hydrated infos = %+v", got)
	}
	if !proto.Equal(infos[0].Log[0], log0) || !proto.Equal(infos[1].Log[0], log1) {
		t.Fatal("externalization mutated input transaction infos")
	}
}

func TestTransactionInfoExternalLogsFailClosed(t *testing.T) {
	block, txHashes := testChainDBEventLogBlockWithTransactions(8, 1)
	log := &corepb.TransactionInfo_Log{Address: []byte{0x51}, Data: bytes.Repeat([]byte{0x52}, 128)}
	encoded, err := marshalTransactionInfosByBlock(8, []*corepb.TransactionInfo{{Id: txHashes[0][:], BlockNumber: 8, Log: []*corepb.TransactionInfo_Log{log}}}, block.Timestamp())
	if err != nil {
		t.Fatal(err)
	}
	external, _, err := ExternalizeTransactionInfoLogs(encoded)
	if err != nil {
		t.Fatal(err)
	}
	db := NewMemoryDatabase()
	if err := WriteBlock(db, block); err != nil {
		t.Fatal(err)
	}
	if err := WriteTransactionInfosRaw(db, block.Number(), external); err != nil {
		t.Fatal(err)
	}
	cdb := NewChainDB(db, NoopAncient{})
	if infos, ok, err := ReadTransactionInfosByBlockStrict(cdb, 8); err == nil || !ok || infos != nil || !strings.Contains(err.Error(), "require cold event-log coverage") {
		t.Fatalf("missing coverage = %+v/%v/%v", infos, ok, err)
	}
	cdb.SetEventLogReader(&recordingCoveredEventLogReader{covered: true})
	if infos, ok, err := ReadTransactionInfosByBlockStrict(cdb, 8); err == nil || !ok || infos != nil || !strings.Contains(err.Error(), "log count 0") {
		t.Fatalf("missing external log = %+v/%v/%v", infos, ok, err)
	}
}

func TestTransactionRetEnvelopeRejectsCorruption(t *testing.T) {
	encoded, err := marshalTransactionInfosByBlock(9, []*corepb.TransactionInfo{{BlockNumber: 9, Log: []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{2}, 128)}}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	external, _, err := ExternalizeTransactionInfoLogs(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{12, 28, transactionRetEnvelopeHeaderSize} {
		corrupt := append([]byte(nil), external...)
		corrupt[offset] ^= 0x80
		if _, err := decodeTransactionRetStorage(corrupt); err == nil {
			t.Fatalf("corruption at offset %d accepted", offset)
		}
	}
}

func TestExternalizeTransactionInfoLogsAvoidsNegativeSavings(t *testing.T) {
	encoded, err := marshalTransactionInfosByBlock(11, []*corepb.TransactionInfo{{BlockNumber: 11, Log: []*corepb.TransactionInfo_Log{{Address: []byte{1}}}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, removed, err := ExternalizeTransactionInfoLogs(encoded)
	if err != nil || removed != 0 || !bytes.Equal(got, encoded) {
		t.Fatalf("tiny-log externalization len=%d removed=%d err=%v", len(got), removed, err)
	}
}

func TestCompactAncientV2ExternalLogsPreservesReceiptBodyMismatch(t *testing.T) {
	block, _ := testChainDBEventLogBlockWithTransactions(12, 1)
	body, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := marshalTransactionInfosByBlock(12, []*corepb.TransactionInfo{{
		Id:          bytes.Repeat([]byte{0xff}, 32),
		BlockNumber: 12,
		Log:         []*corepb.TransactionInfo_Log{{Address: []byte{1}, Data: bytes.Repeat([]byte{2}, 256)}},
	}}, block.Timestamp())
	if err != nil {
		t.Fatal(err)
	}
	got, err := CompactAncientV2RecordWithExternalLogs(ancientTxInfos, 12, encoded, body)
	if err != nil || !bytes.Equal(got, encoded) {
		t.Fatalf("mismatched receipt changed=%v err=%v", !bytes.Equal(got, encoded), err)
	}
}

func TestCompactAncientV2ExternalLogsAllowsEmptyGenesisReceipt(t *testing.T) {
	got, err := CompactAncientV2RecordWithExternalLogs(ancientTxInfos, 0, nil, nil)
	if err != nil || got != nil {
		t.Fatalf("empty genesis receipt = %x/%v, want nil/nil", got, err)
	}
}

func TestExternalizeTransactionInfoLogsLeavesLoglessRowsUnwrapped(t *testing.T) {
	encoded, err := marshalTransactionInfosByBlock(10, []*corepb.TransactionInfo{{BlockNumber: 10, Fee: 1}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, removed, err := ExternalizeTransactionInfoLogs(encoded)
	if err != nil || removed != 0 || !bytes.Equal(got, encoded) {
		t.Fatalf("logless externalization len=%d removed=%d err=%v", len(got), removed, err)
	}
}
