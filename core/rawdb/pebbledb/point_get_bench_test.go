package pebbledb

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

// BenchmarkPointReadOverlappingSST holds the same key/value versions in one
// or four immutable, overlapping L0 tables. Compactions stay disabled in both
// arms; each arm reopens with a fresh cache. This isolates the cost of seeking
// obsolete lower tables, which a single-SST benchmark cannot expose. ReadAt
// counts describe the VFS boundary, not physical device reads.
func BenchmarkPointReadOverlappingSST(b *testing.B) {
	const rows = 1 << 14
	prefix := []byte(commitmentABLegacyPrefix)
	keys := make([][]byte, rows)
	for i := range keys {
		keys[i] = commitmentABPhysicalKey(nil, prefix, uint64(i))
	}
	for _, shape := range []struct {
		name              string
		layers, hotModulo int
	}{
		{"single", 1, 1}, {"overlap-all", 4, 1}, {"overlap-quarter", 4, 4},
	} {
		dir := filepath.Join(b.TempDir(), "db")
		opts := &pebble.Options{Comparer: exactPointComparer, DisableAutomaticCompactions: true,
			MemTableSize: 32 << 20, L0StopWritesThreshold: 32,
			Levels: []pebble.LevelOptions{{FilterPolicy: bloom.FilterPolicy(10), Compression: pebble.NoCompression}}}
		db, err := pebble.Open(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		value := make([]byte, 530)
		for version := 0; version < shape.layers; version++ {
			batch := db.NewBatch()
			for i, key := range keys {
				if version > 0 && i%shape.hotModulo != 0 {
					continue
				}
				commitmentABValue(value, uint64(i), uint64(version))
				if err := batch.Set(key, value, nil); err != nil {
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
			for _, backend := range []string{"iterator", "get"} {
				b.Run(fmt.Sprintf("%s/cache%d/%s", shape.name, cacheMiB, backend), func(b *testing.B) {
					fs := newPhysicalReadFS(vfs.Default, "bench/point-get/"+b.Name()+"/")
					cache := pebble.NewCache(int64(cacheMiB) << 20)
					db, err := pebble.Open(dir, &pebble.Options{Comparer: exactPointComparer, ReadOnly: true, FS: fs, Cache: cache, Logger: panicLogger{}, Levels: opts.Levels})
					cache.Unref()
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := db.Close(); err != nil {
							b.Error(err)
						}
					}()
					wrapped := &Database{db: db, boundedPointGet: backend == "get"}
					snapshot, err := wrapped.NewPointReadSnapshotWithCapacity(1)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := snapshot.Close(); err != nil {
							b.Error(err)
						}
					}()
					cursor, err := snapshot.NewCursor(prefix)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := cursor.Close(); err != nil {
							b.Error(err)
						}
					}()
					consume := func(value []byte) error { benchmarkSeekPrefixValue = value; return nil }
					m := fs.(*physicalReadFS).metrics
					calls, nbytes := m.calls.Snapshot().Count(), m.bytes.Snapshot().Count()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						key := keys[(uint64(i)*2654435761)&(rows-1)]
						if found, err := cursor.View(key, consume); err != nil || !found {
							b.Fatalf("missing key: %v", err)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(m.calls.Snapshot().Count()-calls)/float64(b.N), "ReadAt/op")
					b.ReportMetric(float64(m.bytes.Snapshot().Count()-nbytes)/float64(b.N), "VFS-B/op")
				})
			}
		}
	}
}
