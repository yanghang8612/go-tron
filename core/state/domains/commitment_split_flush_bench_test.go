package domains

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Hide only the optional split-reservation factory for the paired baseline.
// Both arms use the same point-read/cache/ordered fold/commit/durable flush path.
type splitFlushCountedDB struct {
	*tailCountedDB
	factory interface{ NewSplitBatchWithSize(int, int) ethdb.Batch }
}

func (d *splitFlushCountedDB) NewSplitBatchWithSize(hot, history int) ethdb.Batch {
	return d.factory.NewSplitBatchWithSize(hot, history)
}

func BenchmarkCommitmentSplitFlushPipeline(b *testing.B) {
	b.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	b.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", "0")
	for _, mode := range []string{"hot", "cold_scan", "scan", "delayed_read"} {
		f := newTailPipelineFixture(b, mode)
		for _, split := range []bool{false, true} {
			label := "baseline"
			if split {
				label = "split"
			}
			b.Run(mode+"/"+label, func(b *testing.B) {
				b.ReportAllocs()
				var reads, deep uint64
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					hot := f.database(b).KeyValueStore
					history, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
					if err != nil {
						b.Fatal(err)
					}
					routed := rawdb.NewDirectHistoryStore(hot, history)
					counted := &tailCountedDB{KeyValueStore: routed}
					var db ethdb.KeyValueStore = counted
					if split {
						db = &splitFlushCountedDB{tailCountedDB: counted, factory: routed}
					}
					b.StartTimer()
					n, d := f.runWithStore(b, counted, db, 64<<10)
					reads += n
					deep += d
					b.StopTimer()
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(reads)/float64(b.N), "durable_reads/op")
				b.ReportMetric(float64(deep)/float64(b.N), "depth6_7_reads/op")
				b.ReportMetric(8, "blocks/op")
			})
		}
	}
}
