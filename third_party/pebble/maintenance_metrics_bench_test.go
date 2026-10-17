package pebble

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// Many disjoint SSTs and a small cache exercise real table reads and cache
// fills. Key/value generation and worker PRNG recipes are fixed. Parallel
// worker stream lengths/interleaving and the process cache hash seed are not.
func newMaintenanceMetricsBenchmarkDB(b *testing.B) (*DB, [][]byte) {
	b.Helper()
	cache := NewCache(128 << 10)
	db, err := Open("maintenance-bench", &Options{
		FS: vfs.NewMem(), Cache: cache, DisableAutomaticCompactions: true,
		MemTableSize: 1 << 20, L0StopWritesThreshold: 256,
		Levels: []LevelOptions{{BlockSize: 4096, Compression: NoCompression}},
	})
	cache.Unref()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	rng := rand.New(rand.NewSource(731))
	keys := make([][]byte, 4096)
	value := make([]byte, 1024)
	for table := 0; table < 64; table++ {
		batch := db.NewBatch()
		for row := 0; row < 64; row++ {
			id := table*64 + row
			keys[id] = []byte(fmt.Sprintf("key-%08d", id))
			_, _ = rng.Read(value)
			if err := batch.Set(keys[id], value, nil); err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Commit(NoSync); err != nil {
			b.Fatal(err)
		}
		batch.Close()
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	if got := db.Metrics().Levels[0].NumFiles; got != 64 {
		b.Fatalf("fixture SST count=%d, want 64", got)
	}
	// Flush schedules asynchronous table statistics collection. Finish that
	// fixture work before timing either pressure or foreground reads.
	db.mu.Lock()
	for db.mu.tableStats.loading {
		db.mu.tableStats.cond.Wait()
	}
	db.mu.Unlock()
	state := uint64(731)
	for i := 0; i < len(keys); i++ {
		if err := maintenanceMetricsBenchmarkRead(db, keys, &state); err != nil {
			b.Fatal(err)
		}
	}
	return db, keys
}

func maintenanceMetricsBenchmarkRead(db *DB, keys [][]byte, state *uint64) error {
	*state = *state*6364136223846793005 + 1442695040888963407
	key := keys[(*state>>32)%uint64(len(keys))]
	v, closer, err := db.Get(key)
	if err != nil {
		return err
	}
	if len(v) != 1024 {
		_ = closer.Close()
		return fmt.Errorf("wrong value length %d", len(v))
	}
	return closer.Close()
}

func BenchmarkMaintenanceMetricsSampling(b *testing.B) {
	for _, fill := range []bool{false, true} {
		for _, light := range []bool{false, true} {
			b.Run(fmt.Sprintf("readfill_%t/light_%t", fill, light), func(b *testing.B) {
				db, keys := newMaintenanceMetricsBenchmarkDB(b)
				var workers sync.WaitGroup
				var reads atomic.Uint64
				var readErr atomic.Pointer[error]
				stop := make(chan struct{})
				var stopped sync.Once
				stopReaders := func() { stopped.Do(func() { close(stop); workers.Wait() }) }
				defer stopReaders()
				ready := make(chan struct{}, 4)
				if fill {
					for i := 0; i < 4; i++ {
						workers.Add(1)
						go func(id int) {
							defer workers.Done()
							state := uint64(id + 1)
							// Each worker starts the same stream before sampling begins.
							for j := 0; j < 128; j++ {
								if err := maintenanceMetricsBenchmarkRead(db, keys, &state); err != nil {
									readErr.Store(&err)
									ready <- struct{}{}
									return
								}
							}
							ready <- struct{}{}
							for {
								select {
								case <-stop:
									return
								default:
									if err := maintenanceMetricsBenchmarkRead(db, keys, &state); err != nil {
										readErr.Store(&err)
										return
									}
									reads.Add(1)
								}
							}
						}(i)
					}
					for i := 0; i < 4; i++ {
						<-ready
					}
				}
				before := reads.Load()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if light {
						if got := db.MaintenanceMetrics(); got.L0Sublevels != 1 {
							b.Fatalf("unexpected pressure: %+v", got)
						}
					} else if got := db.Metrics(); got.Levels[0].Sublevels != 1 {
						b.Fatalf("unexpected full metrics pressure: %+v", got.Levels[0])
					}
				}
				b.StopTimer()
				after := reads.Load()
				stopReaders()
				if err := readErr.Load(); err != nil {
					b.Fatal(*err)
				}
				if fill {
					b.ReportMetric(float64(after-before)/b.Elapsed().Seconds(), "foreground_reads/s")
				}
			})
		}
	}
}

// Foreground controls use four worker PRNG recipes and sample once every
// 128 reads per worker. The none control performs the same branch/counter work
// without collecting pressure. Worker stream consumption/interleaving is dynamic.
// Saturated sampling above is not production rate.
func BenchmarkMaintenanceMetricsForeground(b *testing.B) {
	for _, mode := range []string{"none", "full", "light"} {
		b.Run(mode, func(b *testing.B) {
			db, keys := newMaintenanceMetricsBenchmarkDB(b)
			var workerID atomic.Uint64
			b.SetParallelism(1)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				state := workerID.Add(1)
				var n uint64
				for pb.Next() {
					if err := maintenanceMetricsBenchmarkRead(db, keys, &state); err != nil {
						b.Error(err)
						return
					}
					n++
					if n%128 == 0 {
						switch mode {
						case "full":
							_ = db.Metrics()
						case "light":
							_ = db.MaintenanceMetrics()
						}
					}
				}
			})
		})
	}
}
