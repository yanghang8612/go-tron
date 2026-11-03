package cache

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
)

// Same source is run before and after the observer; no DB or external workload.
func BenchmarkCausality(b *testing.B) {
	file := base.FileNum(1).DiskFileNum()
	b.Run("GetHit", func(b *testing.B) {
		c := newShards(1<<20, 4)
		defer c.Unref()
		c.Set(1, file, 0, newValue(1024)).Release()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Get(1, file, 0).Release()
		}
	})
	b.Run("SetResize", func(b *testing.B) {
		c := newShards(1<<20, 4)
		defer c.Unref()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Set(1, file, 0, newValue(1024+(i&1)*256)).Release()
		}
	})
	b.Run("CapacityChurn", func(b *testing.B) {
		c := newShards(64<<10, 4)
		defer c.Unref()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.Set(1, file, uint64(i), newValue(1024)).Release()
		}
	})
	b.Run("FileEviction", func(b *testing.B) {
		c := newShards(1<<20, 4)
		defer c.Unref()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 16; j++ {
				c.Set(1, file, uint64(j), newValue(1024)).Release()
			}
			c.EvictFile(1, file)
		}
	})
}

// Stress the real 64-shard snapshot concurrently with a mixed read/churn load.
// The 2ms interval intentionally ensures substantial sampling in short benches;
// it is not the production meter's 3s cadence. Same code runs on both arms.
func BenchmarkCausalityConcurrent64(b *testing.B) {
	c := newShards(1<<20, 64)
	defer c.Unref()
	for i := 0; i < 2048; i++ {
		c.Set(1, base.FileNum(1+i%8).DiskFileNum(), uint64(i), newValue(512)).Release()
	}
	var active, workerID atomic.Int64
	done := make(chan struct{})
	sampled := make(chan [2]int64, 1)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		var total, during int64
		for {
			select {
			case <-done:
				sampled <- [2]int64{total, during}
				return
			case <-ticker.C:
				working := active.Load() > 0
				_ = c.Metrics()
				total++
				if working {
					during++
				}
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		state := uint64(workerID.Add(1))
		active.Add(1)
		defer active.Add(-1)
		for pb.Next() {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			offset := (state >> 8) % 4096
			file := base.FileNum(1 + offset%8).DiskFileNum()
			if state&3 == 0 {
				c.Set(1, file, offset, newValue(512)).Release()
			} else {
				c.Get(1, file, offset).Release()
			}
		}
	})
	b.StopTimer()
	close(done)
	counts := <-sampled
	b.ReportMetric(float64(counts[0]), "metrics_samples")
	b.ReportMetric(float64(counts[1]), "samples_during_workers")
	if b.N >= 100000 && counts[1] == 0 {
		b.Fatal("no actual Metrics sampling during worker load")
	}
}
