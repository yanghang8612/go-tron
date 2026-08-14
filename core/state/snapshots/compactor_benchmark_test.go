package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func BenchmarkStateDomainHistoryCompaction(b *testing.B) {
	benchmarkStateDomainHistoryCompaction(b, benchmarkWriteStateDomainHistorySegment)
}

func BenchmarkStateDomainCompressedHistoryCompaction(b *testing.B) {
	benchmarkStateDomainHistoryCompaction(b, benchmarkWriteCompressedStateDomainHistorySegment)
}

// BenchmarkStateDomainCompressedHistoryCompactionScatteredV6 models mainnet
// transaction order: a large source dictionary whose key IDs jump between far
// apart blocks instead of following lexicographic order. The old V6 merge path
// repeatedly evicted its 64-block dictionary cache for this workload.
func BenchmarkStateDomainCompressedHistoryCompactionScatteredV6(b *testing.B) {
	const recordsPerSegment = 16_384
	first := benchmarkScatteredStateDomainHistoryChanges(1, recordsPerSegment, "a")
	second := benchmarkScatteredStateDomainHistoryChanges(recordsPerSegment+1, recordsPerSegment, "b")
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp(root, "merge-scattered-")
		if err != nil {
			b.Fatal(err)
		}
		refs := append([]SegmentRef{}, writeV6StateDomainHistorySegmentForTest(b, dir, 1, recordsPerSegment, first)...)
		refs = append(refs, writeV6StateDomainHistorySegmentForTest(b, dir, recordsPerSegment+1, recordsPerSegment*2, second)...)
		if err := PublishManifest(dir, NewManifest(1, recordsPerSegment*2, refs)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
		if err != nil {
			b.Fatal(err)
		}
		if !result.Merged {
			b.Fatal("compaction did not merge scattered V6 benchmark segments")
		}
		b.StopTimer()
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkStateDomainHistorySegmentWriter func(b *testing.B, dir string, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) []SegmentRef

func benchmarkStateDomainHistoryCompaction(b *testing.B, writeSegment benchmarkStateDomainHistorySegmentWriter) {
	const (
		txNumsPerSegment = 2_000
		recordsPerTxNum  = 4
	)
	first := benchmarkStateDomainHistoryChanges(1, txNumsPerSegment, recordsPerTxNum)
	second := benchmarkStateDomainHistoryChanges(txNumsPerSegment+1, txNumsPerSegment*2, recordsPerTxNum)
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp(root, "merge-")
		if err != nil {
			b.Fatal(err)
		}
		refs := append([]SegmentRef{}, writeSegment(b, dir, 1, txNumsPerSegment, first)...)
		refs = append(refs, writeSegment(b, dir, txNumsPerSegment+1, txNumsPerSegment*2, second)...)
		if err := PublishManifest(dir, NewManifest(1, txNumsPerSegment*2, refs)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
		if err != nil {
			b.Fatal(err)
		}
		if !result.Merged {
			b.Fatal("compaction did not merge benchmark segments")
		}
		b.StopTimer()
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkStateDomainHistoryChanges(fromTxNum, toTxNum, recordsPerTxNum uint64) []*rawdb.StateDomainChange {
	changes := make([]*rawdb.StateDomainChange, 0, (toTxNum-fromTxNum+1)*recordsPerTxNum)
	for txNum := fromTxNum; txNum <= toTxNum; txNum++ {
		for seq := uint64(1); seq <= recordsPerTxNum; seq++ {
			changes = append(changes, binaryStateDomainChange(txNum, txNum, seq, fmt.Sprintf("key-%08d-%02d", txNum, seq)))
		}
	}
	return changes
}

func benchmarkScatteredStateDomainHistoryChanges(fromTxNum, records uint64, prefix string) []*rawdb.StateDomainChange {
	changes := make([]*rawdb.StateDomainChange, 0, records)
	for i := uint64(0); i < records; i++ {
		// 7919 is odd, hence coprime with the power-of-two record count and a
		// full permutation of the dictionary IDs.
		keyOrdinal := (i * 7919) & (records - 1)
		txNum := fromTxNum + i
		changes = append(changes, binaryStateDomainChange(txNum, txNum, 1, fmt.Sprintf("%s-key-%08d", prefix, keyOrdinal)))
	}
	return changes
}

func benchmarkWriteStateDomainHistorySegment(b *testing.B, dir string, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) []SegmentRef {
	b.Helper()
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryFilesWithAccessor(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      filepath.ToSlash(stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum)),
	}, changes)
	if err != nil {
		b.Fatal(err)
	}
	return []SegmentRef{segRef, accessorRef, idxRef}
}

func benchmarkWriteCompressedStateDomainHistorySegment(b *testing.B, dir string, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) []SegmentRef {
	b.Helper()
	segRef, idxRef, accessorRef, err := writeStateDomainChangeBinaryCompressedSegmentFiles(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: fromTxNum,
		ToTxNum:   toTxNum,
		Path:      filepath.ToSlash(stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum)),
	}, changes)
	if err != nil {
		b.Fatal(err)
	}
	return []SegmentRef{segRef, accessorRef, idxRef}
}

func writeV6StateDomainHistorySegmentForTest(t testing.TB, dir string, fromTxNum, toTxNum uint64, changes []*rawdb.StateDomainChange) []SegmentRef {
	t.Helper()
	segmentData, index, accessorData, err := encodeStateDomainChangeBinarySegmentV6(fromTxNum, toTxNum, normalizeStateDomainChangesForBinary(changes))
	if err != nil {
		t.Fatal(err)
	}
	indexData, err := encodeStateDomainChangeBinaryIndex(fromTxNum, toTxNum, index)
	if err != nil {
		t.Fatal(err)
	}
	historyRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: fromTxNum, ToTxNum: toTxNum, AggregationSteps: 1, Path: filepath.ToSlash(stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum))}
	setStateDomainChangeBinaryRefMetadata(&historyRef, segmentData)
	historyRef.Path = contentAddressedSnapshotPath(historyRef.Path, historyRef.Checksum)
	accessorRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, FromTxNum: fromTxNum, ToTxNum: toTxNum, AggregationSteps: 1, Path: stateDomainChangeBinaryAccessorPath(historyRef.Path)}
	setStateDomainChangeBinaryRefMetadata(&accessorRef, accessorData)
	indexRef := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentInverted, FromTxNum: fromTxNum, ToTxNum: toTxNum, AggregationSteps: 1, Path: stateDomainChangeBinaryIndexPath(historyRef.Path)}
	setStateDomainChangeBinaryRefMetadata(&indexRef, indexData)
	for _, output := range []struct {
		ref  SegmentRef
		data []byte
	}{{historyRef, segmentData}, {accessorRef, accessorData}, {indexRef, indexData}} {
		if err := writeStateDomainChangeBinaryFile(filepath.Join(dir, output.ref.Path), output.data); err != nil {
			t.Fatal(err)
		}
	}
	return []SegmentRef{historyRef, accessorRef, indexRef}
}
