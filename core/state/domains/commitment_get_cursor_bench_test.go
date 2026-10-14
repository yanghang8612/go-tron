package domains

import (
	"os"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Both input arms use this exact test-only file. The original fixture often
// serves the seed from a memtable; the seededSST arm compacts only the synthetic
// seed during untimed setup, so complete root+flush+read also exercises SSTs.
// It configures64 partitions but the small corpus activates only one.
func BenchmarkCommitmentGetCursorPipeline(b *testing.B) {
	b.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	b.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
	b.Setenv("GTRON_COMMITMENT_PREFETCH_OVERLAP", "0")
	for _, seedSST := range []bool{false, true} {
		for _, mode := range []string{"hot", "scan", "delayed_read"} {
			label := "seedMem/"
			if seedSST {
				label = "seedSST/"
			}
			b.Run(label+mode, func(b *testing.B) {
				f := newTailPipelineFixture(b, mode)
				var total, deep uint64
				reads := metrics.GetOrRegisterCounter("disk/physical/read/sst/calls", nil)
				var physical int64
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					b.StopTimer()
					hot := f.database(b).KeyValueStore
					if seedSST {
						if err := hot.Compact(nil, nil); err != nil {
							b.Fatal(err)
						}
						directory := hot.(interface{ Path() string }).Path()
						entries, err := os.ReadDir(directory)
						if err != nil {
							b.Fatal(err)
						}
						present := false
						for _, entry := range entries {
							if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
								present = true
								break
							}
						}
						if !present {
							b.Fatal("seededSST fixture has no actual SST")
						}
					}
					history, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = history.Close() })
					db := &tailCountedDB{KeyValueStore: rawdb.NewDirectHistoryStore(hot, history)}
					start := reads.Snapshot().Count()
					b.StartTimer()
					all, d := f.run(b, db, 64<<10)
					total += all
					deep += d
					b.StopTimer()
					physical += reads.Snapshot().Count() - start
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
				b.ReportMetric(float64(total)/float64(b.N), "durable_reads/op")
				b.ReportMetric(float64(deep)/float64(b.N), "depth6_7_reads/op")
				b.ReportMetric(float64(physical)/float64(b.N), "ReadAt/op")
				b.ReportMetric(8, "blocks/op")
			})
		}
	}
}
