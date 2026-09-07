package snapshots

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Use separately from other CPU benchmarks. This is actual factory + file I/O
// + Sync/Finish, with the same final serialized bytes for every worker count.
// It measures the V3 encoder stage, not full DB/ETL/verification/manifest work.
func BenchmarkCDCWorkerScaling(b *testing.B) {
	for _, input := range cdcBenchmarkInputs() {
		b.Run(input.name, func(b *testing.B) {
			var expected []byte
			for _, workers := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					dir := b.TempDir()
					path := filepath.Join(dir, "history.seg")
					write := func() cdcWriteStats {
						stream, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, workers, "3")
						if err != nil {
							b.Fatal(err)
						}
						w := stream.(*cdcStreamWriter)
						defer w.Abort()
						if _, err := w.Write(input.data); err != nil {
							b.Fatal(err)
						}
						if err := w.Finish(path); err != nil {
							b.Fatal(err)
						}
						return w.stats
					}
					stats := write()
					encoded, err := os.ReadFile(path)
					if err != nil {
						b.Fatal(err)
					}
					if expected == nil {
						expected = encoded
					} else if !bytes.Equal(encoded, expected) {
						b.Fatal("worker count changed serialized file")
					}
					decoded, err := decompressBlockBlob(encoded)
					if err != nil || !bytes.Equal(decoded, input.data) {
						b.Fatal("full bytes oracle mismatch", err)
					}
					b.SetBytes(int64(len(input.data)))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						stats = write()
					}
					b.StopTimer()
					b.ReportMetric(float64(len(encoded)), "encoded-B")
					b.ReportMetric(float64(stats.Pipeline.PeakReservedBytes), "peak-inflight-B")
					b.ReportMetric(float64(stats.Pipeline.PeakPending), "peak-pending")
				})
			}
		})
	}
}
