package pebbledb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

// Same fork, fixture, key multiset and cache budget in both arms. Each operation
// includes creation, all reads, value release and cursor Close. This deliberately
// exposes short-session cost that a long-lived cursor throughput test can hide.
func BenchmarkGetCursorLifecycle(b *testing.B) {
	const rows = 8192
	valueBytes := 128
	if text := os.Getenv("GTRON_GET_CURSOR_BENCH_VALUE"); text != "" {
		n, err := strconv.Atoi(text)
		if err != nil || (n != 32 && n != 128 && n != 530) {
			b.Fatal("value must be32/128/530")
		}
		valueBytes = n
	}
	sorted := os.Getenv("GTRON_GET_CURSOR_BENCH_ORDER") == "sorted"
	observe := os.Getenv("GTRON_GET_CURSOR_BENCH_OBSERVE") == "1"
	keys := make([][]byte, rows)
	want := make([][]byte, rows)
	for i := range keys {
		keys[i] = commitmentABPhysicalKey(nil, []byte(commitmentABLegacyPrefix), uint64(i))
	}
	for _, layers := range []int{1, 4, 8} {
		dir := filepath.Join(b.TempDir(), "db")
		opts := &pebble.Options{Comparer: exactPointComparer, DisableAutomaticCompactions: true, L0StopWritesThreshold: 32,
			MemTableSize: 32 << 20, Levels: []pebble.LevelOptions{{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.NoCompression}}}
		db, err := pebble.Open(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		for wave := 0; wave < layers; wave++ {
			batch := db.NewBatch()
			for i, key := range keys {
				if wave > 0 && i%4 != 0 {
					continue
				}
				v := make([]byte, valueBytes)
				commitmentABValue(v, uint64(i), uint64(wave))
				want[i] = v
				if err := batch.Set(key, v, nil); err != nil {
					b.Fatal(err)
				}
			}
			if err := batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			if err := batch.Close(); err != nil {
				b.Fatal(err)
			}
			if err := db.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		for _, cacheMiB := range []int{1, 64} {
			for _, reads := range []int{0, 1, 4, 16, 256} {
				for _, backend := range []string{"get", "cursor"} {
					b.Run(fmt.Sprintf("L0%d/cache%d/reads%d/%s", layers, cacheMiB, reads, backend), func(b *testing.B) {
						var fs vfs.FS = vfs.Default
						if observe {
							fs = newPhysicalReadFS(vfs.Default, "bench/get-cursor/"+b.Name()+"/")
						}
						cache := pebble.NewCache(int64(cacheMiB) << 20)
						db, err := pebble.Open(dir, &pebble.Options{FS: fs, Comparer: exactPointComparer, Cache: cache, ReadOnly: true, Logger: panicLogger{}, Levels: opts.Levels})
						cache.Unref()
						if err != nil {
							b.Fatal(err)
						}
						defer func() {
							if err := db.Close(); err != nil {
								b.Error(err)
							}
						}()
						s := db.NewSnapshot()
						defer s.Close()
						for _, key := range keys {
							_, cl, err := s.Get(key)
							if err != nil {
								b.Fatal(err)
							}
							if err := cl.Close(); err != nil {
								b.Fatal(err)
							}
						}
						var m *physicalReadMetrics
						var startCalls, startBytes int64
						if observe {
							m = fs.(*physicalReadFS).metrics
							startCalls, startBytes = m.calls.Snapshot().Count(), m.bytes.Snapshot().Count()
						}
						var opens, reuses, fallbacks uint64
						b.ReportAllocs()
						b.ResetTimer()
						for n := 0; n < b.N; n++ {
							var c *pebble.GetCursor
							if backend == "cursor" {
								c, err = s.NewGetCursor()
								if err != nil {
									b.Fatal(err)
								}
							}
							for j := 0; j < reads; j++ {
								index := int((uint64(n*reads+j) * 2654435761) & (rows - 1))
								if sorted {
									index = (n*reads + j) & (rows - 1)
								}
								if c != nil {
									v, e := c.Get(keys[index])
									if e != nil || !bytes.Equal(v, want[index]) {
										b.Fatalf("cursor value mismatch: %v", e)
									}
									if e := c.ReleaseValue(); e != nil {
										b.Fatal(e)
									}
								} else {
									v, cl, e := s.Get(keys[index])
									if e != nil || !bytes.Equal(v, want[index]) {
										b.Fatalf("oracle value mismatch: %v", e)
									}
									if e := cl.Close(); e != nil {
										b.Fatal(e)
									}
								}
							}
							if c != nil {
								stats := c.Stats()
								opens += stats.TableOpens
								reuses += stats.TableReuses
								fallbacks += stats.TableFallbacks
								if err := c.Close(); err != nil {
									b.Fatal(err)
								}
							}
						}
						b.StopTimer()
						denom := float64(b.N * reads)
						b.ReportMetric(float64(reads), "gets/op")
						if observe && reads > 0 {
							b.ReportMetric(float64(m.calls.Snapshot().Count()-startCalls)/denom, "ReadAt/get")
							b.ReportMetric(float64(m.bytes.Snapshot().Count()-startBytes)/denom, "VFS-B/get")
						}
						if backend == "cursor" && reads > 0 {
							b.ReportMetric(float64(opens)/denom, "table_opens/get")
							b.ReportMetric(float64(reuses)/denom, "table_reuses/get")
							b.ReportMetric(float64(fallbacks)/denom, "fallbacks/get")
						}
					})
				}
			}
		}
	}
}
