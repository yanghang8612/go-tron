package rawdb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStateChangeChunksParallelEncodingIsIdentical(t *testing.T) {
	rows := chunkHistoryRows(2<<20, 4)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	want, useful := encodeStateChangeChunksWithWorkers(raw, 1)
	if !useful {
		t.Fatal("fixture did not compress")
	}
	for _, workers := range []int{2, 4, 8} {
		got, ok := encodeStateChangeChunksWithWorkers(raw, workers)
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("workers %d changed encoded representation", workers)
		}
		decoded, err := decodeStateDomainChangeBlockStorage(got)
		if err != nil || !bytes.Equal(decoded, raw) {
			t.Fatalf("workers %d changed RLP: %v", workers, err)
		}
	}
}

func BenchmarkStateChangeChunksParallel(b *testing.B) {
	rows := chunkHistoryRows(2<<20, 4)
	raw := encodeBorrowedStateDomainChangeTestBlock(b, rows)
	for _, workers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encoded, ok := encodeStateChangeChunksWithWorkers(raw, workers)
				if !ok {
					b.Fatal("fixture did not compress")
				}
				b.ReportMetric(float64(len(encoded)), "stored-B")
			}
		})
	}
}

func BenchmarkStateChangeChunksProductionGate(b *testing.B) {
	rows := chunkHistoryRows(2<<20, 4)
	raw := encodeBorrowedStateDomainChangeTestBlock(b, rows)
	prior := stateChangeBlockChunkEncoding.Load()
	b.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("enabled=%t", enabled), func(b *testing.B) {
			stateChangeBlockChunkEncoding.Store(enabled)
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				encoded, _ := encodeStateDomainChangeBlockStorageForChanges(raw, rows)
				b.ReportMetric(float64(len(encoded)), "stored-B")
			}
		})
	}
}
