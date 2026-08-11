package snapshots

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestInspectEventLogSpaceV2PhysicalAndCandidates(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	addressA := eventLogTestAddress(0x21)
	topicA := common.Hash{0xa1}
	topicB := common.Hash{0xb1}
	block1, infos1 := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addressA, Topics: [][]byte{topicA[:]}, Data: []byte{1}},
		{Address: addressA, Topics: [][]byte{topicA[:], topicB[:]}, Data: make([]byte, 127)},
	})
	block2, infos2 := eventLogTestBlock(t, 2, []*corepb.TransactionInfo_Log{
		{Address: addressA, Data: make([]byte, 128)},
	})
	for _, block := range []*coretypes.Block{block1, block2} {
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock: %v", err)
		}
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos1); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 1: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 2, infos2); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock 2: %v", err)
	}
	if _, err := NewAggregator(dir).BuildEventLogs(db, 1, 1); err != nil {
		t.Fatalf("BuildEventLogs 1: %v", err)
	}
	if _, err := NewAggregator(dir).BuildEventLogs(db, 2, 2); err != nil {
		t.Fatalf("BuildEventLogs 2: %v", err)
	}

	inspection, err := InspectEventLogSpace(dir, EventLogSpaceInspectOptions{SampleSegments: 0, Context: context.Background()})
	if err != nil {
		t.Fatalf("InspectEventLogSpace: %v", err)
	}
	if !inspection.SampledAll || inspection.SampledEventSegments != 2 || inspection.SampledIndexSegments != 2 {
		t.Fatalf("selection = all:%v events:%d indexes:%d, want true/2/2", inspection.SampledAll, inspection.SampledEventSegments, inspection.SampledIndexSegments)
	}
	if inspection.Duplicates.Rows != 3 || inspection.PayloadSizes.Count != 3 || inspection.TopicCounts.Count != 3 {
		t.Fatalf("row distributions = rows:%d payload:%+v topics:%+v", inspection.Duplicates.Rows, inspection.PayloadSizes, inspection.TopicCounts)
	}
	if inspection.TopicCounts.Min != 0 || inspection.TopicCounts.Max != 2 {
		t.Fatalf("topic distribution = %+v, want min=0 max=2", inspection.TopicCounts)
	}
	if inspection.SamplePhysical.Header != 2*eventLogHeaderV2Size || inspection.SamplePhysical.FixedRowIndex != 3*eventLogIndexEntrySize {
		t.Fatalf("physical = %+v, want two V2 headers and three fixed rows", inspection.SamplePhysical)
	}
	mainBytes := inspection.SamplePhysical.Header + inspection.SamplePhysical.FixedRowIndex + inspection.SamplePhysical.ProtobufPayload + inspection.SamplePhysical.EmbeddedAddressPostings + inspection.SamplePhysical.EmbeddedTopicPostings
	if mainBytes+inspection.SamplePhysical.ExternalSidecar != inspection.SamplePhysical.Total {
		t.Fatalf("physical components total %d + sidecar %d, want %d", mainBytes, inspection.SamplePhysical.ExternalSidecar, inspection.SamplePhysical.Total)
	}
	if len(inspection.Candidates) != 6 {
		t.Fatalf("candidate count = %d, want 6", len(inspection.Candidates))
	}
	for _, candidate := range inspection.Candidates {
		if candidate.EstimatedPhysicalBytes == 0 || candidate.ComparedPhysicalBytes != inspection.SamplePhysical.Total {
			t.Fatalf("candidate totals = %+v", candidate)
		}
		if candidate.MaxPointDecompress == 0 || candidate.MaxPointReadBytes == 0 || candidate.MaxSingleKeyLookupRead == 0 {
			t.Fatalf("candidate random lookup bounds = %+v", candidate)
		}
		if candidate.SegmentModel != "selected-segments-merged-into-one" {
			t.Fatalf("candidate segment model = %q", candidate.SegmentModel)
		}
	}
	if len(inspection.Merge) != 6 || inspection.Merge[1].SampleAddressPostings >= inspection.Merge[0].SampleAddressPostings {
		t.Fatalf("merge projections = %+v, want factor-two address posting collapse", inspection.Merge)
	}
}

func TestInspectEventLogSpaceV1Physical(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	topic := common.Hash{0x44}
	block, infos := eventLogTestBlock(t, 7, []*corepb.TransactionInfo_Log{{
		Address: eventLogTestAddress(0x41),
		Topics:  [][]byte{topic[:]},
		Data:    []byte{7, 8, 9},
	}})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 7, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	v2, err := BuildEventLogSegmentFromChain(db, dir, "log/source-v2.seg", 7, 7)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain: %v", err)
	}
	v2seg, err := OpenEventLogSegment(dir, v2)
	if err != nil {
		t.Fatalf("OpenEventLogSegment: %v", err)
	}
	entry, err := readEventLogIndexEntryAt(v2seg.file, eventLogIndexEntryOffset(v2seg.header, 0))
	if err != nil {
		t.Fatalf("read entry: %v", err)
	}
	payload, err := v2seg.readLogPayload(entry)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if err := v2seg.Close(); err != nil {
		t.Fatalf("close V2: %v", err)
	}

	relPath := "log/legacy-v1.seg"
	absPath := filepath.Join(dir, relPath)
	file, err := os.Create(absPath)
	if err != nil {
		t.Fatalf("create V1: %v", err)
	}
	var header [eventLogHeaderV1Size]byte
	copy(header[:8], eventLogMagicV1[:])
	binary.BigEndian.PutUint64(header[8:16], 7)
	binary.BigEndian.PutUint64(header[16:24], 7)
	binary.BigEndian.PutUint64(header[24:32], 1)
	binary.BigEndian.PutUint64(header[32:40], eventLogHeaderV1Size)
	payloadOffset := uint64(eventLogHeaderV1Size + eventLogIndexEntrySize)
	binary.BigEndian.PutUint64(header[40:48], payloadOffset)
	entry.offset = payloadOffset
	entry.length = uint64(len(payload))
	var row [eventLogIndexEntrySize]byte
	encodeEventLogIndexEntry(row[:], entry)
	if _, err := file.Write(header[:]); err != nil {
		t.Fatalf("write V1 header: %v", err)
	}
	if _, err := file.Write(row[:]); err != nil {
		t.Fatalf("write V1 row: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		t.Fatalf("write V1 payload: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close V1: %v", err)
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadata(absPath)
	if err != nil {
		t.Fatalf("V1 metadata: %v", err)
	}
	ref := SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 7, ToTxNum: 7, Path: relPath, Size: size, Checksum: checksum}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	inspection, err := InspectEventLogSpace(dir, EventLogSpaceInspectOptions{})
	if err != nil {
		t.Fatalf("InspectEventLogSpace V1: %v", err)
	}
	segment := inspection.Segments[0]
	if segment.Version != 1 || segment.Physical.Header != eventLogHeaderV1Size || segment.Physical.FixedRowIndex != eventLogIndexEntrySize || segment.Physical.ProtobufPayload != uint64(len(payload)) || segment.Physical.EmbeddedAddressPostings != 0 || segment.Physical.EmbeddedTopicPostings != 0 {
		t.Fatalf("V1 physical = %+v", segment)
	}
}

func TestEventLogInspectHelpers(t *testing.T) {
	if got := checkedCandidateSavings(100, 125); got != -250 {
		t.Fatalf("negative savings milli = %d, want -250", got)
	}
	if got := checkedCandidateSavings(math.MaxUint64, 0); got != 1000 {
		t.Fatalf("max savings milli = %d, want 1000", got)
	}
	state := new(eventLogCandidatePostingState)
	state.add(0, 0)
	state.add(1, 0)
	state.add(2, 1)
	state.add(8, 2)
	postings := map[string]*eventLogCandidatePostingState{"key": state}
	if got := eventLogCandidateLookup(postings, 3, 0).Postings; got != 3 {
		t.Fatalf("factor-one merge postings = %d, want 3 segment groups", got)
	}
	if got := eventLogCandidateLookup(postings, 3, 1).Postings; got != 2 {
		t.Fatalf("factor-two merge postings = %d, want 2 segment groups", got)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectEventLogSpace(t.TempDir(), EventLogSpaceInspectOptions{Context: cancelled}); err == nil {
		t.Fatal("InspectEventLogSpace without manifest returned nil error")
	}
}

func TestEventLogManagerMixedV1V2Regression(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	address := eventLogTestAddress(0x61)
	topic := common.Hash{0xcc}
	for blockNum := uint64(1); blockNum <= 2; blockNum++ {
		block, infos := eventLogTestBlock(t, blockNum, []*corepb.TransactionInfo_Log{{
			Address: address,
			Topics:  [][]byte{topic[:]},
			Data:    []byte{byte(blockNum)},
		}})
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	v2first, err := BuildEventLogSegmentFromChain(db, dir, "log/mixed-source-1.seg", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 1: %v", err)
	}
	v1first := rewriteEventLogSegmentAsV1(t, dir, v2first, "log/mixed-v1-1.seg")
	v2second, err := BuildEventLogSegmentFromChain(db, dir, "log/mixed-v2-2.seg", 2, 2)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain 2: %v", err)
	}
	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{v1first, v2second}, "log/mixed-index-1-2.idx")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(0, 0, []SegmentRef{v1first, v2second, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	var rows []EventLog
	if err := mgr.IterateEventLogs(1, 2, EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(address)},
		Topics:    [][]common.Hash{{topic}},
	}, func(row EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs mixed V1/V2: %v", err)
	}
	if len(rows) != 2 || rows[0].BlockNum != 1 || rows[1].BlockNum != 2 || rows[0].LogIndex != 0 || rows[1].LogIndex != 0 {
		t.Fatalf("mixed rows = %+v, want blocks 1,2 in order", rows)
	}
	if rows[0].Log.GetData()[0] != 1 || rows[1].Log.GetData()[0] != 2 {
		t.Fatalf("mixed row payloads = %x/%x, want 01/02", rows[0].Log.GetData(), rows[1].Log.GetData())
	}
}

func TestInspectEventLogSpaceFromManifestPinsGeneration(t *testing.T) {
	dir := t.TempDir()
	db := rawdb.NewMemoryChainDB()
	block, infos := eventLogTestBlock(t, 1, []*corepb.TransactionInfo_Log{{Address: eventLogTestAddress(0x71), Data: []byte{1}}})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if _, err := NewAggregator(dir).BuildEventLogs(db, 1, 1); err != nil {
		t.Fatalf("BuildEventLogs: %v", err)
	}
	pinned, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest: %v", err)
	}
	replacement := NewManifest(0, 0, nil)
	replacement.Generation = pinned.Generation + 1
	if err := PublishManifest(dir, replacement); err != nil {
		t.Fatalf("Publish replacement manifest: %v", err)
	}
	inspection, err := InspectEventLogSpaceFromManifest(dir, pinned, EventLogSpaceInspectOptions{})
	if err != nil {
		t.Fatalf("InspectEventLogSpaceFromManifest: %v", err)
	}
	if inspection.ManifestGeneration != pinned.Generation || inspection.Duplicates.Rows != 1 {
		t.Fatalf("pinned inspection generation=%d rows=%d, want %d/1", inspection.ManifestGeneration, inspection.Duplicates.Rows, pinned.Generation)
	}
	current, err := InspectEventLogSpace(dir, EventLogSpaceInspectOptions{})
	if err != nil {
		t.Fatalf("InspectEventLogSpace current: %v", err)
	}
	if current.ManifestGeneration != replacement.Generation || current.Duplicates.Rows != 0 {
		t.Fatalf("current inspection generation=%d rows=%d, want %d/0", current.ManifestGeneration, current.Duplicates.Rows, replacement.Generation)
	}
}

func rewriteEventLogSegmentAsV1(t *testing.T, dir string, source SegmentRef, relPath string) SegmentRef {
	t.Helper()
	seg, err := OpenEventLogSegment(dir, source)
	if err != nil {
		t.Fatalf("OpenEventLogSegment: %v", err)
	}
	entries := make([]eventLogIndexEntry, seg.header.rowCount)
	payloads := make([][]byte, seg.header.rowCount)
	for i := uint64(0); i < seg.header.rowCount; i++ {
		entries[i], err = readEventLogIndexEntryAt(seg.file, eventLogIndexEntryOffset(seg.header, i))
		if err != nil {
			_ = seg.Close()
			t.Fatalf("read event-log row %d: %v", i, err)
		}
		payloads[i], err = seg.readLogPayload(entries[i])
		if err != nil {
			_ = seg.Close()
			t.Fatalf("read event-log payload %d: %v", i, err)
		}
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("close source segment: %v", err)
	}
	absPath := filepath.Join(dir, relPath)
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		t.Fatalf("mkdir V1 parent: %v", err)
	}
	file, err := os.Create(absPath)
	if err != nil {
		t.Fatalf("create V1: %v", err)
	}
	var header [eventLogHeaderV1Size]byte
	copy(header[:8], eventLogMagicV1[:])
	binary.BigEndian.PutUint64(header[8:16], source.FromTxNum)
	binary.BigEndian.PutUint64(header[16:24], source.ToTxNum)
	binary.BigEndian.PutUint64(header[24:32], uint64(len(entries)))
	binary.BigEndian.PutUint64(header[32:40], eventLogHeaderV1Size)
	payloadOffset := uint64(eventLogHeaderV1Size) + uint64(len(entries))*eventLogIndexEntrySize
	binary.BigEndian.PutUint64(header[40:48], payloadOffset)
	if _, err := file.Write(header[:]); err != nil {
		t.Fatalf("write V1 header: %v", err)
	}
	offset := payloadOffset
	for i, entry := range entries {
		entry.offset = offset
		entry.length = uint64(len(payloads[i]))
		var row [eventLogIndexEntrySize]byte
		encodeEventLogIndexEntry(row[:], entry)
		if _, err := file.Write(row[:]); err != nil {
			t.Fatalf("write V1 row %d: %v", i, err)
		}
		offset += entry.length
	}
	for i, payload := range payloads {
		if _, err := file.Write(payload); err != nil {
			t.Fatalf("write V1 payload %d: %v", i, err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close V1: %v", err)
	}
	size, checksum, err := stateDomainChangeBinaryFileMetadata(absPath)
	if err != nil {
		t.Fatalf("V1 metadata: %v", err)
	}
	return SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: source.FromTxNum, ToTxNum: source.ToTxNum, Path: relPath, Size: size, Checksum: checksum}
}

func BenchmarkEventLogSpaceSyntheticFixture(b *testing.B) {
	dir := b.TempDir()
	db := rawdb.NewMemoryChainDB()
	const (
		blocks       = 256
		logsPerBlock = 10
	)
	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		logs := make([]*corepb.TransactionInfo_Log, 0, logsPerBlock)
		for logIndex := 0; logIndex < logsPerBlock; logIndex++ {
			address := eventLogTestAddress(byte((blockNum*17 + uint64(logIndex)) % 251))
			topicCount := (int(blockNum) + logIndex) % 5
			topics := make([][]byte, topicCount)
			for position := range topics {
				topic := common.Hash{}
				binary.BigEndian.PutUint64(topic[0:8], blockNum)
				binary.BigEndian.PutUint64(topic[8:16], uint64(logIndex))
				binary.BigEndian.PutUint64(topic[16:24], uint64(position))
				topics[position] = topic[:]
			}
			data := make([]byte, 16+(int(blockNum)+logIndex*31)%768)
			state := blockNum ^ uint64(logIndex+1)*0x9e3779b97f4a7c15
			for i := range data {
				state ^= state << 13
				state ^= state >> 7
				state ^= state << 17
				data[i] = byte(state)
			}
			logs = append(logs, &corepb.TransactionInfo_Log{Address: address, Topics: topics, Data: data})
		}
		block, infos := eventLogTestBlock(b, blockNum, logs)
		if err := rawdb.WriteBlock(db, block); err != nil {
			b.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			b.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	result, err := NewAggregator(dir).BuildEventLogs(db, 1, blocks)
	if err != nil {
		b.Fatalf("BuildEventLogs: %v", err)
	}
	var physical uint64
	for _, ref := range result.Segments {
		physical += ref.Size
	}
	b.SetBytes(int64(physical))
	b.ReportAllocs()
	b.ResetTimer()
	var inspection *EventLogSpaceInspection
	for i := 0; i < b.N; i++ {
		inspection, err = InspectEventLogSpace(dir, EventLogSpaceInspectOptions{})
		if err != nil {
			b.Fatalf("InspectEventLogSpace: %v", err)
		}
	}
	b.StopTimer()
	b.Logf("synthetic limitation: one tx/block, deterministic pseudo-random data, cyclic addresses, 0-4 topics; not mainnet representative")
	b.Logf("rows=%d current=%d header=%d fixed=%d payload=%d embeddedAddress=%d embeddedTopic=%d sidecar=%d payloadP50=%d payloadP95=%d payloadP99=%d",
		inspection.Duplicates.Rows, inspection.SamplePhysical.Total, inspection.SamplePhysical.Header, inspection.SamplePhysical.FixedRowIndex,
		inspection.SamplePhysical.ProtobufPayload, inspection.SamplePhysical.EmbeddedAddressPostings, inspection.SamplePhysical.EmbeddedTopicPostings,
		inspection.SamplePhysical.ExternalSidecar, inspection.PayloadSizes.P50, inspection.PayloadSizes.P95, inspection.PayloadSizes.P99)
	for _, candidate := range inspection.Candidates {
		b.Logf("candidate block=%d hash=%s physical=%d savingsMilli=%d maxRead=%d maxDecompress=%d maxSingleKeyLookup=%d",
			candidate.PayloadBlockSize, candidate.BlockHashSource, candidate.EstimatedPhysicalBytes, candidate.SavingsMilli,
			candidate.MaxPointReadBytes, candidate.MaxPointDecompress, candidate.MaxSingleKeyLookupRead)
	}
}
