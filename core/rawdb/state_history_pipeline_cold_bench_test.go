package rawdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Controlled complete-trio comparison: same 16-block source, two complete
// authenticated passes, key dictionary, record CDC/compression, companions and
// internal build verification. Same one-trio layout, owned reads and auto
// compression. Excludes publication/pruning and is NOT production throughput.
// Reuse varies source bytes only; this pipeline performs no memoization.
func BenchmarkStateHistorySharedPipelineColdBuild(b *testing.B) {
	for _, shape := range []string{"Unique", "HighReuseRaw", "HighReuseSnappy"} {
		b.Run(shape, func(b *testing.B) {
			db, from, to, total := rawdb.OwnedReadBenchmarkFixtureForTest(b, shape)
			for _, workers := range []int{0, 2, 4, 8} {
				pipeline := workers != 0
				slots := workers
				if slots == 0 {
					slots = 2
				}
				name := "Serial"
				if pipeline {
					name = fmt.Sprintf("Pipeline%d", workers)
				}
				b.Run(name, func(b *testing.B) {
					dir := b.TempDir()
					b.ReportAllocs()
					b.SetBytes(int64(total))
					for b.Loop() {
						view, release, err := rawdb.AcquireStateHistoryReadView(db)
						if err != nil {
							b.Fatal(err)
						}
						refs, err := snapshots.BuildDiagnosticStateHistoryTrioWithReadWorkersContext(context.Background(), view, dir, from, to, from, to, "state-domain-change-pipeline-bench.seg", "auto", etl.Options{}, pipeline, slots)
						closeErr := release()
						if err != nil || closeErr != nil || len(refs) != 3 {
							b.Fatalf("refs=%d err=%v close=%v", len(refs), err, closeErr)
						}
					}
				})
			}
		})
	}
}
