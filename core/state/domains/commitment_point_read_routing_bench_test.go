package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Keep both real engine snapshots pinned by DirectHistoryStore. Eight complete
// folds, layer publications and canonical flushes consume commitment hot keys;
// this exposes unused history-side cursor storage without changing either cut.
// Compare isolated baseline, routing-only and routing+lazy-capacity source arms.
func BenchmarkCommitmentPointReadRoutingPipeline(b *testing.B) {
	b.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	b.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	b.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", "0")
	for _, mode := range []string{"hot", "scan", "delayed_read"} {
		b.Run(mode, func(b *testing.B) {
			f := newTailPipelineFixture(b, mode)
			var total, deep uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				hot := f.database(b).KeyValueStore
				history, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = history.Close() })
				db := &tailCountedDB{KeyValueStore: rawdb.NewDirectHistoryStore(hot, history)}
				b.StartTimer()
				n, d := f.run(b, db, 64<<10)
				total += n
				deep += d
				b.StopTimer()
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(total)/float64(b.N), "durable_reads/op")
			b.ReportMetric(float64(deep)/float64(b.N), "depth6_7_reads/op")
			b.ReportMetric(8, "blocks/op")
		})
	}
}
