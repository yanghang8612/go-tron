package snapshots

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
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
	indexStats, err := InspectEventLogIndexes(dir)
	if err != nil {
		t.Fatalf("InspectEventLogIndexes: %v", err)
	}
	if len(indexStats.Segments) != 1 ||
		indexStats.Address.Keys != 2 || indexStats.Address.Postings != 2 || indexStats.Address.MaxPostingsPerKey != 1 ||
		indexStats.Address.AveragePostingsPerKeyMilli != 1000 || indexStats.Address.SingletonKeys != 2 || indexStats.Address.MultiPostingKeys != 0 ||
		indexStats.Topic.Keys != 2 || indexStats.Topic.Postings != 2 || indexStats.Topic.MaxPostingsPerKey != 1 ||
		indexStats.Topic.AveragePostingsPerKeyMilli != 1000 || indexStats.Topic.SingletonKeys != 2 || indexStats.Topic.MultiPostingKeys != 0 {
		t.Fatalf("event log index stats = %+v, want two address/topic keys with one posting each", indexStats)
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

func TestEventLogIndexLookupStatsAddRecomputesDistribution(t *testing.T) {
	var total EventLogIndexLookupStats
	total.add(EventLogIndexLookupStats{
		Keys:                       2,
		Postings:                   3,
		AveragePostingsPerKeyMilli: 1500,
		MaxPostingsPerKey:          2,
		SingletonKeys:              1,
		MultiPostingKeys:           1,
	})
	total.add(EventLogIndexLookupStats{
		Keys:                       1,
		Postings:                   3,
		AveragePostingsPerKeyMilli: 3000,
		MaxPostingsPerKey:          3,
		MultiPostingKeys:           1,
	})

	if total.Keys != 3 || total.Postings != 6 || total.AveragePostingsPerKeyMilli != 2000 ||
		total.MaxPostingsPerKey != 3 || total.SingletonKeys != 1 || total.MultiPostingKeys != 2 {
		t.Fatalf("total stats = %+v, want keys=3 postings=6 avg=2000 max=3 singleton=1 multi=2", total)
	}
}

func TestBuildEventLogSegmentWithOptionsUsesETLScratch(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "snapshot")
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x18)
	topic := common.Hash{0x18}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x18}},
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x19}},
	})
	if err := rawdb.WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}

	etlTemp := filepath.Join(root, "etl-scratch")
	ref, err := BuildEventLogSegmentFromChainWithOptions(db, dir, "log/event-log-1-1.seg", 1, 1, RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChainWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if err := CheckEventLogSegment(dir, ref); err != nil {
		t.Fatalf("CheckEventLogSegment: %v", err)
	}
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatalf("OpenEventLogSegment: %v", err)
	}
	defer seg.Close()
	var rows []EventLog
	if err := seg.IterateLogs(1, 1, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addr)},
		Topics:    [][]common.Hash{{topic}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateLogs: %v", err)
	}
	if len(rows) != 2 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x18}) || !bytes.Equal(rows[1].Log.GetData(), []byte{0x19}) {
		t.Fatalf("ETL event-log rows = %+v, want two ordered rows", rows)
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
	covered, err = mgr.EventLogIndexedRangeCovered(1, 3)
	if err != nil || covered {
		t.Fatalf("EventLogIndexedRangeCovered without index = %v/%v, want false/nil", covered, err)
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

func TestBuildEventLogIndexSegmentWithOptionsUsesETLScratch(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "snapshot")
	db := rawdb.NewMemoryChainDB()
	addrA := eventLogTestAddress(0x62)
	addrB := eventLogTestAddress(0x63)
	topicA := common.Hash{0x62}
	topicB := common.Hash{0x63}
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

	etlTemp := filepath.Join(root, "etl-index-scratch")
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegmentsWithOptions(dir, []SegmentRef{ref1, ref2}, "", RestoreETLOptions{
		TempDir:     etlTemp,
		BufferLimit: 1,
	})
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegmentsWithOptions: %v", err)
	}
	if _, err := os.Stat(etlTemp); err != nil {
		t.Fatalf("ETL temp parent stat: %v", err)
	}
	if err := CheckEventLogIndexSegment(dir, indexRef); err != nil {
		t.Fatalf("CheckEventLogIndexSegment: %v", err)
	}
	if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, []SegmentRef{ref1, ref2}); err != nil {
		t.Fatalf("verifyEventLogIndexSegmentAgainstEventLogs: %v", err)
	}
	index, err := OpenEventLogIndexSegment(dir, indexRef)
	if err != nil {
		t.Fatalf("OpenEventLogIndexSegment: %v", err)
	}
	defer index.Close()
	starts, used, err := index.CandidateSegmentStarts(EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addrB)},
		Topics:    [][]common.Hash{{topicB}},
	})
	if err != nil {
		t.Fatalf("CandidateSegmentStarts: %v", err)
	}
	if !used || len(starts) != 1 || starts[0] != ref2.FromTxNum {
		t.Fatalf("CandidateSegmentStarts = %v used=%v, want [%d]/true", starts, used, ref2.FromTxNum)
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
	covered, err = mgr.EventLogIndexedRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogIndexedRangeCovered before removal = %v, %v; want true, nil", covered, err)
	}
	var eventRef, indexRef SegmentRef
	for _, ref := range result.Segments {
		switch ref.Kind {
		case SegmentEventLog:
			eventRef = ref
		case SegmentEventLogIndex:
			indexRef = ref
		}
	}
	if err := os.Remove(filepath.Join(dir, indexRef.Path)); err != nil {
		t.Fatalf("Remove event-log-index segment: %v", err)
	}
	covered, err = mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered after index removal = %v, %v; want true, nil", covered, err)
	}
	covered, err = mgr.EventLogIndexedRangeCovered(1, 1)
	if err == nil || covered {
		t.Fatalf("EventLogIndexedRangeCovered after index removal = %v, %v; want false, error", covered, err)
	}
	if err := os.Remove(filepath.Join(dir, eventRef.Path)); err != nil {
		t.Fatalf("Remove segment: %v", err)
	}
	covered, err = mgr.EventLogRangeCovered(1, 1)
	if err == nil || covered {
		t.Fatalf("EventLogRangeCovered after removal = %v, %v; want false, error", covered, err)
	}
}

func TestEventLogIndexedRangeCoveredRejectsStaleIndexPostings(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x45)
	topic := common.Hash{0xcd}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x05}},
	})
	if err := rawdb.WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	eventRef, err := BuildEventLogSegmentFromChain(db, dir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	badIndexRef, err := writeEventLogIndexSegment(dir, SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLogIndex,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      "log/event-log-index-empty-1-1.idx",
	}, nil, nil)
	if err != nil {
		t.Fatalf("writeEventLogIndexSegment empty: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{eventRef, badIndexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	covered, err = mgr.EventLogIndexedRangeCovered(1, 1)
	if err == nil || covered {
		t.Fatalf("EventLogIndexedRangeCovered stale index = %v/%v, want false/error", covered, err)
	}
}

func TestEventLogRangeCoveredForFilterRejectsStaleEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x46)
	topic := common.Hash{0xce}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x06}},
	})
	if err := rawdb.WriteBlock(db, block1); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	eventRef, err := BuildEventLogSegmentFromChain(db, dir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	badIndexRef, err := writeEventLogIndexSegment(dir, SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLogIndex,
		FromTxNum: 1,
		ToTxNum:   1,
		Path:      "log/event-log-index-empty-filter-1-1.idx",
	}, nil, nil)
	if err != nil {
		t.Fatalf("writeEventLogIndexSegment empty: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{eventRef, badIndexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCoveredForFilter(1, 1, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addr)},
		Topics:    [][]common.Hash{{topic}},
	})
	if err == nil || covered {
		t.Fatalf("EventLogRangeCoveredForFilter stale empty index = %v/%v, want false/error", covered, err)
	}
}

func TestEventLogRangeCoveredForFilterRejectsMissingCandidateSegment(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addr := eventLogTestAddress(0x47)
	topic := common.Hash{0xcf}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x07}},
	})
	block2, infos2 := eventLogTestBlock(t, 2, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x08}},
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
	badIndexRef, err := writeEventLogIndexSegment(dir, SegmentRef{
		Dataset:   SegmentDatasetEventLog,
		Kind:      SegmentEventLogIndex,
		FromTxNum: 1,
		ToTxNum:   2,
		Path:      "log/event-log-index-missing-candidate-1-2.idx",
	}, map[string][]uint64{
		string(eventLogAddressLookupKey(common.BytesToAddress(addr))): {1},
	}, map[string][]uint64{
		string(eventLogTopicLookupKey(0, topic)): {1},
	})
	if err != nil {
		t.Fatalf("writeEventLogIndexSegment partial: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref1, ref2, badIndexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	filter := EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addr)},
		Topics:    [][]common.Hash{{topic}},
	}
	covered, err := mgr.EventLogRangeCoveredForFilter(1, 2, filter)
	if err == nil || covered {
		t.Fatalf("EventLogRangeCoveredForFilter partial index = %v/%v, want false/error", covered, err)
	}
	if err := mgr.IterateEventLogs(1, 2, filter, func(EventLog) (bool, error) {
		return true, nil
	}); err == nil {
		t.Fatal("IterateEventLogs accepted stale partial event-log-index")
	}
}

func TestEventLogSegmentBuildRejectsMissingBlock(t *testing.T) {
	_, err := BuildEventLogSegmentFromChain(rawdb.NewMemoryChainDB(), t.TempDir(), "", 1, 1)
	if err == nil {
		t.Fatal("BuildEventLogSegmentFromChain accepted missing block")
	}
}

func TestEventLogSegmentBuildRejectsMissingTransactionInfoCoverage(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	block, _ := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{{
		Address: eventLogTestAddress(0x91),
		Data:    []byte{0x01},
	}})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if _, err := BuildEventLogSegmentFromChain(db, t.TempDir(), "", 1, 1); err == nil {
		t.Fatal("BuildEventLogSegmentFromChain accepted a tx-bearing block without TransactionInfo coverage")
	}
}

func TestEventLogSegmentBuildRejectsMismatchedTransactionInfo(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	block, infos := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{{
		Address: eventLogTestAddress(0x92),
		Data:    []byte{0x02},
	}})
	infos[0].Id = bytes.Repeat([]byte{0xee}, common.HashLength)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if _, err := BuildEventLogSegmentFromChain(db, t.TempDir(), "", 1, 1); err == nil {
		t.Fatal("BuildEventLogSegmentFromChain accepted mismatched TransactionInfo id")
	}
}

func TestEventLogSegmentBuildRejectsMismatchedTransactionInfoBlockNumber(t *testing.T) {
	hot := rawdb.NewMemoryDatabase()
	block, infos := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{{
		Address: eventLogTestAddress(0x93),
		Data:    []byte{0x03},
	}})
	infos[0].BlockNumber = 2
	if err := rawdb.WriteBlock(hot, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	db := rawdb.NewChainDB(hot, eventLogTestAncientWithTxInfos(t, 1, &corepb.TransactionRet{
		BlockNumber:     1,
		Transactioninfo: infos,
	}))
	if _, err := BuildEventLogSegmentFromChain(db, t.TempDir(), "", 1, 1); err == nil || !strings.Contains(err.Error(), "transaction info block number 2") {
		t.Fatalf("BuildEventLogSegmentFromChain error = %v, want block number mismatch", err)
	}
}

type eventLogTestAncient struct {
	rows map[string]map[uint64][]byte
}

func eventLogTestAncientWithTxInfos(t *testing.T, blockNum uint64, ret *corepb.TransactionRet) eventLogTestAncient {
	t.Helper()
	data, err := proto.Marshal(ret)
	if err != nil {
		t.Fatalf("marshal ancient TransactionRet: %v", err)
	}
	return eventLogTestAncient{rows: map[string]map[uint64][]byte{
		rawdb.AncientTxInfosTable: {blockNum: data},
	}}
}

func (a eventLogTestAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if rows := a.rows[kind]; rows != nil {
		if data, ok := rows[number]; ok {
			return append([]byte(nil), data...), nil
		}
	}
	return nil, rawdb.ErrNotInAncient
}

func (a eventLogTestAncient) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, count)
	var total uint64
	for i := uint64(0); i < count; i++ {
		data, err := a.Ancient(kind, start+i)
		if err != nil {
			if len(out) != 0 && err == rawdb.ErrNotInAncient {
				break
			}
			return nil, err
		}
		if maxBytes > 0 && len(out) != 0 && total+uint64(len(data)) > maxBytes {
			break
		}
		out = append(out, data)
		total += uint64(len(data))
	}
	if len(out) == 0 {
		return nil, rawdb.ErrNotInAncient
	}
	return out, nil
}

func (a eventLogTestAncient) AncientCount(kind string) (uint64, error) {
	rows := a.rows[kind]
	var max uint64
	for number := range rows {
		if number+1 > max {
			max = number + 1
		}
	}
	return max, nil
}

func (a eventLogTestAncient) HasAncient(kind string, number uint64) (bool, error) {
	rows := a.rows[kind]
	_, ok := rows[number]
	return ok, nil
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
