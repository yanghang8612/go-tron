package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func BenchmarkStateDomainHistoryCompaction(b *testing.B) {
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
		refs := append([]SegmentRef{}, benchmarkWriteStateDomainHistorySegment(b, dir, 1, txNumsPerSegment, first)...)
		refs = append(refs, benchmarkWriteStateDomainHistorySegment(b, dir, txNumsPerSegment+1, txNumsPerSegment*2, second)...)
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
