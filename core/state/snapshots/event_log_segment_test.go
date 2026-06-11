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
	if len(result.Segments) != 2 {
		t.Fatalf("BuildEventLogs segments = %d, want event-log and event-log-index", len(result.Segments))
	}
	var ref, indexRef SegmentRef
	for _, candidate := range result.Segments {
		switch candidate.Kind {
		case SegmentEventLog:
			ref = candidate
		case SegmentEventLogIndex:
			indexRef = candidate
		}
	}
	if ref.Dataset != SegmentDatasetEventLog || ref.Kind != SegmentEventLog {
		t.Fatalf("ref family = %s/%s, want %s/%s", ref.Dataset, ref.Kind, SegmentDatasetEventLog, SegmentEventLog)
	}
	if indexRef.Dataset != SegmentDatasetEventLog || indexRef.Kind != SegmentEventLogIndex {
		t.Fatalf("index ref family = %s/%s, want %s/%s", indexRef.Dataset, indexRef.Kind, SegmentDatasetEventLog, SegmentEventLogIndex)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref metadata missing: size=%d checksum=%q", ref.Size, ref.Checksum)
	}
	if err := CheckEventLogSegment(dir, ref); err != nil {
		t.Fatalf("CheckEventLogSegment: %v", err)
	}
	if err := CheckEventLogIndexSegment(dir, indexRef); err != nil {
		t.Fatalf("CheckEventLogIndexSegment: %v", err)
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

func TestEventLogManagerIteratesContinuousSegmentsWithFilter(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addrA := eventLogTestAddress(0x31)
	addrB := eventLogTestAddress(0x41)
	topicA := common.Hash{0xa1}
	topicB := common.Hash{0xb2}
	blocks := make([]*coretypes.Block, 0, 3)
	infosByBlock := make(map[uint64][]*corepb.TransactionInfo)

	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addrA, Topics: [][]byte{topicA[:]}, Data: []byte{0x01}},
	})
	blocks = append(blocks, block1)
	infosByBlock[1] = infos1
	block2, infos2 := eventLogTestBlock(t, 2, []*corepb.TransactionInfo_Log{
		{Address: addrA, Topics: [][]byte{topicB[:]}, Data: []byte{0x02}},
		{Address: addrB, Topics: [][]byte{topicA[:]}, Data: []byte{0x20}},
	})
	blocks = append(blocks, block2)
	infosByBlock[2] = infos2
	block3, infos3 := eventLogTestBlock(t, 3, []*corepb.TransactionInfo_Log{
		{Address: addrA, Topics: [][]byte{topicA[:]}, Data: []byte{0x03}},
	})
	blocks = append(blocks, block3)
	infosByBlock[3] = infos3
	for _, block := range blocks {
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", block.Number(), err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, block.Number(), infosByBlock[block.Number()]); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", block.Number(), err)
		}
	}

	ref1, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref2, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-2-3.seg", 2, 3)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2-3: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref2, ref1})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 3)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered(1,3) = %v/%v, want true/nil", covered, err)
	}
	covered, err = mgr.EventLogRangeCovered(1, 4)
	if err != nil || covered {
		t.Fatalf("EventLogRangeCovered(1,4) = %v/%v, want false/nil", covered, err)
	}

	var rows []EventLog
	if err := mgr.IterateEventLogs(1, 3, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addrA)},
		Topics:    [][]common.Hash{{topicA}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs addrA/topicA: %v", err)
	}
	if len(rows) != 2 || rows[0].BlockNum != 1 || rows[1].BlockNum != 3 ||
		!bytes.Equal(rows[0].Log.GetData(), []byte{0x01}) || !bytes.Equal(rows[1].Log.GetData(), []byte{0x03}) {
		t.Fatalf("addrA/topicA rows = %+v, want block1 and block3 in order", rows)
	}

	rows = nil
	if err := mgr.IterateEventLogs(1, 3, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addrA)},
		Topics:    [][]common.Hash{{topicB}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs addrA/topicB: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 2 || rows[0].TxIndex != 0 || rows[0].LogIndex != 0 ||
		!bytes.Equal(rows[0].Log.GetData(), []byte{0x02}) {
		t.Fatalf("addrA/topicB rows = %+v, want block2 first log", rows)
	}

	visited := 0
	if err := mgr.IterateEventLogs(1, 3, EventLogFilter{Topics: [][]common.Hash{{topicA}}}, func(row EventLog) (bool, error) {
		visited++
		return false, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs short-circuit across segments: %v", err)
	}
	if visited != 1 {
		t.Fatalf("short-circuit visited %d rows, want 1", visited)
	}
}

func TestEventLogManagerUsesGlobalIndexToSkipUnrelatedSegments(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addrA := eventLogTestAddress(0x51)
	addrB := eventLogTestAddress(0x61)
	topicA := common.Hash{0xa5}
	topicB := common.Hash{0xb6}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addrA, Topics: [][]byte{topicA[:]}, Data: []byte{0x01}},
	})
	block2, infos2 := eventLogTestBlock(t, 2, []*corepb.TransactionInfo_Log{
		{Address: addrB, Topics: [][]byte{topicB[:]}, Data: []byte{0x02}},
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
	ref1, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref2, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-2-2.seg", 2, 2)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2: %v", err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1, ref2}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref1, ref2, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, ref2.Path)); err != nil {
		t.Fatalf("remove unrelated event-log segment: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	filter := EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addrA)},
		Topics:    [][]common.Hash{{topicA}},
	}
	covered, err := mgr.EventLogRangeCoveredForFilter(1, 2, filter)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCoveredForFilter with global index = %v/%v, want true/nil", covered, err)
	}
	var rows []EventLog
	if err := mgr.IterateEventLogs(1, 2, filter, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs with global index: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x01}) {
		t.Fatalf("indexed rows = %+v, want only block1", rows)
	}
}

func TestEventLogIndexBuildRejectsGappedSegments(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x71)
	topic := common.Hash{0x77}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x01}},
	})
	block3, infos3 := eventLogTestBlock(t, 3, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x03}},
	})
	for _, block := range []*coretypes.Block{block1, block3} {
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 3, infos3); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 3: %v", err)
	}
	ref1, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-1-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	ref3, err := BuildEventLogSegmentFromChain(db, dir, "log/event-log-3-3.seg", 3, 3)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 3: %v", err)
	}
	if _, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1, ref3}, "log/event-log-index-1-3.idx"); err == nil {
		t.Fatal("BuildEventLogIndexSegmentFromEventLogSegments accepted gapped event-log coverage")
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

func TestEventLogRangeCoveredRequiresReadableSegments(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x44)
	topic := common.Hash{0xcc}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x04}},
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
	if len(result.Segments) != 2 {
		t.Fatalf("BuildEventLogs segments = %d, want event-log and event-log-index", len(result.Segments))
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered before removal = %v, %v; want true, nil", covered, err)
	}
	if err := os.Remove(filepath.Join(dir, result.Segments[0].Path)); err != nil {
		t.Fatalf("Remove segment: %v", err)
	}
	covered, err = mgr.EventLogRangeCovered(1, 1)
	if err == nil || covered {
		t.Fatalf("EventLogRangeCovered after removal = %v, %v; want false, error", covered, err)
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
