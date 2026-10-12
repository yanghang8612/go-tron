package snapshots

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// This benchmark also runs unchanged on c6c8fd32. It measures the production
// cache-only topology and complete V4 chain build, including verification.
func BenchmarkColdBuildOptimization(b *testing.B) {
	old := runtime.GOMAXPROCS(16)
	defer runtime.GOMAXPROCS(old)
	for _, paired := range []bool{false, true} {
		b.Run(fmt.Sprintf("cache_pair_%v", paired), func(b *testing.B) {
			db, err := rawdb.NewPebbleDB(b.TempDir(), 64, 64)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			const blocks, txs = 128, 32
			seedParallelHistoryEvent(b, db, blocks, txs, 2, 1024)
			root := b.TempDir()
			var total, event time.Duration
			var actualPaired int
			var checksum string
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r := parallelHistoryEventRunner(&coldBuilderChain{db: rawdb.NewChainDB(db, nil), solidified: blocks + 1, syncRemaining: 100000, syncRemainingOK: true}, filepath.Join(root, fmt.Sprint(i)), blocks, txs)
				r.cfg.HistorySharedChunkCache = true
				r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
					return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 16000, MemoryAvailableBytes: 8 << 30}
				}
				r.cfg.ParallelHistoryEventReady = func() bool { return paired }
				for _, stage := range []rawdb.StageID{rawdb.StageSnapshotBuild, rawdb.StageSnapshotHistory, rawdb.StageSnapshotAccessor, rawdb.StageSnapshotEventLogBuild} {
					if err := rawdb.DeleteStageProgress(db, stage); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				result, err := r.OnePass()
				b.StopTimer()
				if err != nil || !result.Built || !result.HistorySharedChunkCache {
					b.Fatalf("build: %+v %v", result, err)
				}
				current := ""
				for _, ref := range result.Segments {
					current += ref.Checksum
				}
				if checksum == "" {
					checksum = current
				} else if checksum != current {
					b.Fatal("output changed")
				}
				total += result.BuildDuration
				event += result.EventLogDuration
				if result.HistoryEventParallel {
					actualPaired++
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "build-wall-ns/op")
			b.ReportMetric(float64(event.Nanoseconds())/float64(b.N), "event-ns/op")
			b.ReportMetric(float64(actualPaired)/float64(b.N), "paired/op")
		})
	}
}

// Isolate the cache's serial contribution, including a source larger than its
// charged memory budget that falls back to the original second pass. Both cases use the same canonical Pebble entry point
// as production and run unchanged on the baseline revision.
func BenchmarkColdEventReplay(b *testing.B) {
	for _, payload := range []int{512, 8192} {
		b.Run(fmt.Sprintf("payload_%d", payload), func(b *testing.B) {
			db, err := rawdb.NewPebbleDB(b.TempDir(), 64, 64)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			const blocks, txs = 64, 32
			seedParallelHistoryEvent(b, db, blocks, txs, 1, payload)
			chain := rawdb.NewChainDB(db, nil)
			root := b.TempDir()
			checksum := ""
			b.ReportAllocs()
			b.SetBytes(blocks * txs * int64(payload))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := BuildEventLogV4SegmentFromChain(chain, filepath.Join(root, fmt.Sprint(i)), "", 1, blocks)
				if err != nil {
					b.Fatal(err)
				}
				if checksum == "" {
					checksum = ref.Checksum
				} else if checksum != ref.Checksum {
					b.Fatal("event identity changed")
				}
			}
		})
	}
}
