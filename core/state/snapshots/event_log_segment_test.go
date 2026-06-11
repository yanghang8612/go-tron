package snapshots

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEventLogSegmentBuildVerifyLookup(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addrA := eventLogTestAddress(0x11)
	addrB := eventLogTestAddress(0x22)
	topicA := common.Hash{0xaa}
	topicB := common.Hash{0xbb}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addrA, Topics: [][]byte{topicA[:]}, Data: []byte{0x01}},
		{Address: addrB, Topics: [][]byte{topicB[:]}, Data: []byte{0x02}},
	})
	block2, infos2 := eventLogTestBlock(t, 2, []*corepb.TransactionInfo_Log{
		{Address: addrA, Topics: [][]byte{topicB[:]}, Data: []byte{0x03}},
	})
	for _, block := range []*coretypes.Block{block1, block2} {
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 2, infos2); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 2: %v", err)
	}

	result, err := NewAggregator(dir).BuildEventLogs(db, 1, 2)
	if err != nil {
		t.Fatalf("BuildEventLogs: %v", err)
	}
	if len(result.Segments) != 1 {
		t.Fatalf("BuildEventLogs segments = %d, want 1", len(result.Segments))
	}
	ref := result.Segments[0]
	if ref.Dataset != SegmentDatasetEventLog || ref.Kind != SegmentEventLog {
		t.Fatalf("ref family = %s/%s, want %s/%s", ref.Dataset, ref.Kind, SegmentDatasetEventLog, SegmentEventLog)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref metadata missing: size=%d checksum=%q", ref.Size, ref.Checksum)
	}
	if err := CheckEventLogSegment(dir, ref); err != nil {
		t.Fatalf("CheckEventLogSegment: %v", err)
	}
	if _, err := VerifyManifestFiles(dir, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatalf("VerifyManifestFiles: %v", err)
	}

	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatalf("OpenEventLogSegment: %v", err)
	}
	defer seg.Close()
	var rows []EventLog
	if err := seg.IterateLogs(1, 2, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addrA)},
		Topics:    [][]common.Hash{{topicB}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 2 || rows[0].TxIndex != 0 || rows[0].LogIndex != 0 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x03}) {
		t.Fatalf("filtered rows = %+v, want block2 addrA/topicB", rows)
	}
	if rows[0].BlockHash != block2.Hash() || rows[0].TxHash != block2.Transactions()[0].Hash() {
		t.Fatalf("row hashes = block %x tx %x, want block %x tx %x", rows[0].BlockHash, rows[0].TxHash, block2.Hash(), block2.Transactions()[0].Hash())
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	rows = nil
	if err := mgr.IterateEventLogs(1, 1, EventLogFilter{Addresses: []common.Address{common.BytesToAddress(addrB)}}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("manager IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || rows[0].LogIndex != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x02}) {
		t.Fatalf("manager rows = %+v, want block1 second log", rows)
	}

	visited := 0
	if err := mgr.IterateEventLogs(1, 2, EventLogFilter{}, func(row EventLog) (bool, error) {
		visited++
		return false, nil
	}); err != nil {
		t.Fatalf("manager IterateEventLogs short-circuit: %v", err)
	}
	if visited != 1 {
		t.Fatalf("short-circuit visited %d rows, want 1", visited)
	}
}

func TestEventLogSegmentTopicLookupSkipsNonCandidatePayloads(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x33)
	topicA := common.Hash{0xaa}
	topicB := common.Hash{0xbb}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topicA[:]}, Data: []byte{0x01}},
		{Address: addr, Topics: [][]byte{topicB[:]}, Data: []byte{0x02}},
	})
	if err := rawdb.WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	result, err := NewAggregator(dir).BuildEventLogs(db, 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogs: %v", err)
	}
	ref := result.Segments[0]
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatalf("OpenEventLogSegment: %v", err)
	}
	firstEntry, err := readEventLogIndexEntryAt(seg.file, eventLogIndexEntryOffset(seg.header, 0))
	if err != nil {
		t.Fatalf("read first index entry: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, ref.Path), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteAt(bytes.Repeat([]byte{0xff}, int(firstEntry.length)), int64(firstEntry.offset)); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt first payload: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close corrupting file: %v", err)
	}

	seg, err = OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatalf("OpenEventLogSegment after corruption: %v", err)
	}
	defer seg.Close()
	var rows []EventLog
	if err := seg.IterateLogs(1, 1, EventLogFilter{Topics: [][]common.Hash{{topicB}}}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateLogs with topic lookup: %v", err)
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x02}) {
		t.Fatalf("topic lookup rows = %+v, want only topicB row", rows)
	}
}

func TestEventLogSegmentBuildRejectsMissingBlock(t *testing.T) {
	_, err := BuildEventLogSegmentFromChain(rawdb.NewMemoryChainDB(), t.TempDir(), "", 1, 1)
	if err == nil {
		t.Fatal("BuildEventLogSegmentFromChain accepted missing block")
	}
}

func eventLogTestBlock(t *testing.T, number uint64, logs []*corepb.TransactionInfo_Log) (*coretypes.Block, []*corepb.TransactionInfo) {
	t.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
			Data:       []byte{byte(number)},
		},
	}
	tx := coretypes.NewTransactionFromPB(txPB)
	info := &corepb.TransactionInfo{
		Id:             append([]byte(nil), tx.Hash().Bytes()...),
		BlockNumber:    int64(number),
		BlockTimeStamp: int64(30_000 + number),
		Log:            logs,
	}
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	return block, []*corepb.TransactionInfo{info}
}

func eventLogTestAddress(seed byte) []byte {
	out := make([]byte, common.AccountIDLength)
	for i := range out {
		out[i] = seed + byte(i)
	}
	return out
}
