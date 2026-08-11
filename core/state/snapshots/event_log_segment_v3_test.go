package snapshots

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEventLogV3BuildVerifyLookupAndMixedManifest(t *testing.T) {
	dir := t.TempDir()
	addrA := common.BytesToAddress(eventLogTestAddress(0x31))
	addrB := common.BytesToAddress(eventLogTestAddress(0x32))
	topicA := common.Hash{0xa1}
	topicB := common.Hash{0xb2}
	rows := []EventLog{
		eventLogV3TestRow(1, 0, 0, addrA, common.Hash{0x11}, common.Hash{0x21}, topicA, bytes.Repeat([]byte{0x01}, 200)),
		eventLogV3TestRow(1, 0, 1, addrB, common.Hash{0x11}, common.Hash{0x21}, topicB, bytes.Repeat([]byte{0x02}, 33000)),
		eventLogV3TestRow(2, 0, 0, addrA, common.Hash{0x12}, common.Hash{0x22}, topicB, bytes.Repeat([]byte{0x03}, 300)),
		{BlockNum: 2, TxIndex: 1, LogIndex: 1, TxHash: common.Hash{0x13}, BlockHash: common.Hash{0x22}, Address: addrB, Log: &corepb.TransactionInfo_Log{Address: append([]byte(nil), addrB[:]...)}},
	}
	ref, err := BuildEventLogV3SegmentFromReader(eventLogRowsReader{rows: rows}, dir, "", 1, 2)
	if err != nil {
		t.Fatalf("BuildEventLogV3SegmentFromReader: %v", err)
	}
	if err := CheckEventLogSegment(dir, ref); err != nil {
		t.Fatalf("CheckEventLogSegment V3: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[:8], eventLogMagicV3[:]) {
		t.Fatalf("magic = %q, want V3", raw[:8])
	}

	indexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments V3: %v", err)
	}
	manifest := NewManifest(1, 2, []SegmentRef{ref, indexRef})
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	var got []EventLog
	err = mgr.IterateEventLogs(1, 2, EventLogFilter{Addresses: []common.Address{addrA}, Topics: [][]common.Hash{{topicB}}}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	})
	if err != nil {
		t.Fatalf("IterateEventLogs V3: %v", err)
	}
	if len(got) != 1 || got[0].BlockNum != 2 || !bytes.Equal(got[0].Log.GetAddress(), addrA[:]) || len(got[0].Log.GetData()) != 300 {
		t.Fatalf("V3 filtered rows = %+v", got)
	}
}

func TestEventLogV3RejectsCorruptPayloadFrame(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x41))
	topic := common.Hash{0xc1}
	row := eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{2}, topic, []byte("payload"))
	ref, err := BuildEventLogV3SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, ref.Path), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	header, err := readEventLogHeader(file)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := readEventLogV3FrameAt(file, header.v3.payloadDirOffset, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := file.ReadAt(one[:], int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := file.WriteAt(one[:], int64(frame.dataOff)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	// Clear metadata so the semantic/frame checker, rather than the expected
	// whole-file checksum, demonstrates the local corruption boundary.
	ref.Checksum = ""
	if err := CheckEventLogSegment(dir, ref); err == nil {
		t.Fatal("CheckEventLogSegment accepted corrupt V3 payload frame")
	}
}

func TestEventLogV3AllowsAddressOnlyLogWithEmptyStrippedPayload(t *testing.T) {
	dir := t.TempDir()
	address := common.BytesToAddress(eventLogTestAddress(0x42))
	row := EventLog{BlockNum: 1, TxHash: common.Hash{1}, BlockHash: common.Hash{2}, Address: address, Log: &corepb.TransactionInfo_Log{Address: append([]byte(nil), address[:]...)}}
	ref, err := BuildEventLogV3SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogV3SegmentFromReader empty payload: %v", err)
	}
	if err := CheckEventLogSegment(dir, ref); err != nil {
		t.Fatalf("CheckEventLogSegment empty payload: %v", err)
	}
}

func TestEventLogV3NormalizesTwentyByteTVMAddress(t *testing.T) {
	dir := t.TempDir()
	rawAddress := bytes.Repeat([]byte{0x73}, common.AddressLength-1)
	address := common.BytesToAddress(rawAddress)
	row := EventLog{BlockNum: 1, TxHash: common.Hash{1}, BlockHash: common.Hash{2}, Address: address, Log: &corepb.TransactionInfo_Log{Address: rawAddress, Data: []byte{1}}}
	ref, err := BuildEventLogV3SegmentFromReader(eventLogRowsReader{rows: []EventLog{row}}, dir, "", 1, 1)
	if err != nil {
		t.Fatalf("BuildEventLogV3SegmentFromReader 20-byte address: %v", err)
	}
	seg, err := OpenEventLogSegment(dir, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	var got EventLog
	if err := seg.IterateLogs(1, 1, EventLogFilter{}, func(row EventLog) (bool, error) { got = row; return true, nil }); err != nil {
		t.Fatalf("IterateLogs 20-byte address: %v", err)
	}
	if got.Address != address || eventLogAddress(got.Log.GetAddress()) != address {
		t.Fatalf("normalized address = %x payload=%x, want %x", got.Address, got.Log.GetAddress(), address)
	}
}

func TestEventLogBuildVersionSelectionPreservesLegacyDefault(t *testing.T) {
	address := common.BytesToAddress(eventLogTestAddress(0x43))
	row := eventLogV3TestRow(1, 0, 0, address, common.Hash{1}, common.Hash{2}, common.Hash{3}, []byte("payload"))
	reader := eventLogRowsReader{rows: []EventLog{row}}

	legacyDir := t.TempDir()
	legacy, err := NewAggregator(legacyDir).BuildEventLogsFromReader(reader, 1, 1)
	if err != nil {
		t.Fatalf("legacy BuildEventLogsFromReader: %v", err)
	}
	legacySegment, err := OpenEventLogSegment(legacyDir, eventLogRefs(legacy.Manifest)[0])
	if err != nil {
		t.Fatal(err)
	}
	legacyVersion := legacySegment.header.version
	_ = legacySegment.Close()
	if legacyVersion != EventLogSegmentVersion {
		t.Fatalf("legacy API version = %d, want V2", legacyVersion)
	}

	v3Dir := t.TempDir()
	v3, err := NewAggregator(v3Dir).BuildEventLogsFromReaderWithBuildOptions(reader, 1, 1, EventLogBuildOptions{Version: EventLogSegmentV3Version})
	if err != nil {
		t.Fatalf("V3 BuildEventLogsFromReaderWithBuildOptions: %v", err)
	}
	v3Segment, err := OpenEventLogSegment(v3Dir, eventLogRefs(v3.Manifest)[0])
	if err != nil {
		t.Fatal(err)
	}
	v3Version := v3Segment.header.version
	_ = v3Segment.Close()
	if v3Version != EventLogSegmentV3Version {
		t.Fatalf("explicit API version = %d, want V3", v3Version)
	}
	if len(eventLogIndexRefs(v3.Manifest)) != 1 {
		t.Fatalf("V3 manifest has no external event-log-index companion: %+v", v3.Manifest.Segments)
	}
	if _, err := NewAggregator(t.TempDir()).BuildEventLogsFromReaderWithBuildOptions(reader, 1, 1, EventLogBuildOptions{Version: 4}); err == nil {
		t.Fatal("unsupported event-log version was accepted")
	}
}

func TestBuildEventLogV3DirectlyFromChainNormalizesTVMAddress(t *testing.T) {
	dir := t.TempDir()
	chain := rawdb.NewMemoryChainDB()
	rawAddress := bytes.Repeat([]byte{0x74}, common.AddressLength-1)
	topic := common.Hash{0xd4}
	block, infos := coldBuilderEventLogBlock(t, 1, []*corepb.TransactionInfo_Log{{
		Address: rawAddress,
		Topics:  [][]byte{topic[:]},
		Data:    []byte("direct-v3"),
	}})
	if err := rawdb.WriteBlock(chain, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(chain, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	result, err := NewAggregator(dir).BuildEventLogsWithBuildOptions(chain, 1, 1, EventLogBuildOptions{Version: EventLogSegmentV3Version})
	if err != nil {
		t.Fatalf("BuildEventLogsWithBuildOptions: %v", err)
	}
	if len(eventLogRefs(result.Manifest)) != 1 || len(eventLogIndexRefs(result.Manifest)) != 1 {
		t.Fatalf("direct V3 refs = %+v", result.Manifest.Segments)
	}
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	normalized := common.BytesToAddress(rawAddress)
	var got []EventLog
	if err := mgr.IterateEventLogs(1, 1, EventLogFilter{Addresses: []common.Address{normalized}, Topics: [][]common.Hash{{topic}}}, func(row EventLog) (bool, error) {
		got = append(got, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(got) != 1 || got[0].Address != normalized || !bytes.Equal(got[0].Log.GetData(), []byte("direct-v3")) {
		t.Fatalf("direct V3 rows = %+v", got)
	}
}

func TestMigrateEventLogsV3BuildOnlyThenPublish(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x51))
	topic := common.Hash{0xd1}
	rows1 := []EventLog{eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{11}, topic, []byte("one"))}
	rows2 := []EventLog{eventLogV3TestRow(2, 0, 0, addr, common.Hash{2}, common.Hash{12}, topic, []byte("two"))}
	ref1, err := BuildEventLogSegmentFromReader(eventLogRowsReader{rows: rows1}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref2, err := BuildEventLogSegmentFromReader(eventLogRowsReader{rows: rows2}, dir, "", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	idx1, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1}, "")
	if err != nil {
		t.Fatal(err)
	}
	idx2, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref2}, "")
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifest(1, 2, []SegmentRef{ref1, ref2, idx1, idx2})
	manifest.Generation = 7
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	preview, err := MigrateEventLogsV3(dir, EventLogV3MigrationOptions{FromBlock: 1, Merge: 2})
	if err != nil {
		t.Fatalf("build-only migration: %v", err)
	}
	if preview.Published || preview.SourceSegments != 2 || preview.Generation != 7 {
		t.Fatalf("build-only result = %+v", preview)
	}
	unchanged, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Generation != 7 || len(eventLogRefs(unchanged)) != 2 {
		t.Fatalf("build-only changed manifest: %+v", unchanged)
	}
	published, err := MigrateEventLogsV3(dir, EventLogV3MigrationOptions{FromBlock: 1, Merge: 2, Publish: true})
	if err != nil {
		t.Fatalf("published migration: %v", err)
	}
	if !published.Published || published.Generation != 8 {
		t.Fatalf("published result = %+v", published)
	}
	active, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventLogRefs(active)) != 1 || len(eventLogIndexRefs(active)) != 1 {
		t.Fatalf("active refs after migration: %+v", active.Segments)
	}
	seg, err := OpenEventLogSegment(dir, eventLogRefs(active)[0])
	if err != nil {
		t.Fatal(err)
	}
	if seg.header.version != EventLogSegmentV3Version {
		t.Fatalf("active event-log version = %d", seg.header.version)
	}
	_ = seg.Close()
	inspection, err := InspectEventLogSpace(dir, EventLogSpaceInspectOptions{SampleSegments: 1, Context: context.Background()})
	if err != nil {
		t.Fatalf("InspectEventLogSpace V3: %v", err)
	}
	if len(inspection.Segments) != 1 || inspection.Segments[0].Version != EventLogSegmentV3Version || inspection.Segments[0].Physical.MainSegment != published.V3MainBytes {
		t.Fatalf("V3 inspection = %+v", inspection.Segments)
	}
	if _, err := MigrateEventLogsV3(dir, EventLogV3MigrationOptions{FromBlock: 1, Merge: 1, Publish: true}); err != nil {
		t.Fatalf("idempotent V3 migration rerun: %v", err)
	}
}

func TestMigrateSingleEventLogV3PreservesCrossingGlobalIndex(t *testing.T) {
	dir := t.TempDir()
	addr := common.BytesToAddress(eventLogTestAddress(0x61))
	topic := common.Hash{0xe1}
	rows1 := []EventLog{eventLogV3TestRow(1, 0, 0, addr, common.Hash{1}, common.Hash{21}, topic, []byte("one"))}
	rows2 := []EventLog{eventLogV3TestRow(2, 0, 0, addr, common.Hash{2}, common.Hash{22}, topic, []byte("two"))}
	ref1, err := BuildEventLogSegmentFromReader(eventLogRowsReader{rows: rows1}, dir, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	ref2, err := BuildEventLogSegmentFromReader(eventLogRowsReader{rows: rows2}, dir, "", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	globalIndex, err := BuildEventLogIndexSegmentFromEventLogSegments(dir, []SegmentRef{ref1, ref2}, "")
	if err != nil {
		t.Fatal(err)
	}
	manifest := NewManifest(1, 2, []SegmentRef{ref1, ref2, globalIndex})
	manifest.Generation = 11
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}

	result, err := MigrateEventLogsV3(dir, EventLogV3MigrationOptions{FromBlock: 1, ToBlock: 1, ToBlockSet: true, Publish: true})
	if err != nil {
		t.Fatalf("single V3 migration with crossing index: %v", err)
	}
	if !result.Published || result.PreservedIndexes != 1 || result.PreservedIndexBytes != globalIndex.Size || result.V3IndexBytes != 0 || len(result.Segments) != 1 {
		t.Fatalf("migration result = %+v", result)
	}
	active, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	activeIndexes := eventLogIndexRefs(active)
	if len(activeIndexes) != 1 || activeIndexes[0] != globalIndex {
		t.Fatalf("global index was not preserved: %+v", activeIndexes)
	}
	activeLogs := eventLogRefs(active)
	if len(activeLogs) != 2 {
		t.Fatalf("active event logs = %+v", activeLogs)
	}
	first, err := OpenEventLogSegment(dir, activeLogs[0])
	if err != nil {
		t.Fatal(err)
	}
	if first.header.version != EventLogSegmentV3Version {
		t.Fatalf("first version = %d", first.header.version)
	}
	_ = first.Close()
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	var blocks []uint64
	err = mgr.IterateEventLogs(1, 2, EventLogFilter{Addresses: []common.Address{addr}, Topics: [][]common.Hash{{topic}}}, func(row EventLog) (bool, error) {
		blocks = append(blocks, row.BlockNum)
		return true, nil
	})
	if err != nil {
		t.Fatalf("filtered query through preserved global index: %v", err)
	}
	if !equalUint64Slices(blocks, []uint64{1, 2}) {
		t.Fatalf("blocks = %v", blocks)
	}
}

func eventLogV3TestRow(block, tx, log uint64, address common.Address, txHash, blockHash, topic common.Hash, data []byte) EventLog {
	return EventLog{
		BlockNum: block, TxIndex: tx, LogIndex: log, TxHash: txHash, BlockHash: blockHash, Address: address,
		Log: &corepb.TransactionInfo_Log{Address: append([]byte(nil), address[:]...), Topics: [][]byte{append([]byte(nil), topic[:]...)}, Data: data},
	}
}
