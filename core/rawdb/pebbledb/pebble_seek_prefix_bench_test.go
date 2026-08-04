package pebbledb

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

var benchmarkSeekPrefixValue []byte

// BenchmarkPebbleExactReadStrategy compares DB.Get with reusable iterator
// seeks while preserving the full-key prefix Bloom semantics used by exact
// point reads. It guards the production investigation against repeating the
// regressed SeekGE experiment, which bypassed those filters.
func BenchmarkPebbleExactReadStrategy(b *testing.B) {
	const entries = 1 << 16
	comparer := *pebble.DefaultComparer
	// Treat the whole physical key as its prefix. This matches the table
	// filters already built for bytewise point keys and enables the public
	// iterator's exact-key SeekPrefixGE path.
	comparer.Split = func(key []byte) int { return len(key) }
	db, err := pebble.Open(filepath.Join(b.TempDir(), "exact-read"), &pebble.Options{Comparer: &comparer})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	keys := make([][]byte, entries)
	batch := db.NewBatch()
	for i := range keys {
		key := make([]byte, 64)
		copy(key, "state-commitment-branch-v1-")
		binary.BigEndian.PutUint64(key[len(key)-8:], uint64(i))
		keys[i] = key
		if err := batch.Set(key, bytes.Repeat([]byte{byte(i)}, 530), nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Commit(nil); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}

	for _, sorted := range []bool{false, true} {
		name := "random"
		if sorted {
			name = "sorted"
		}
		keyAt := func(i int) []byte {
			if sorted {
				return keys[i&(entries-1)]
			}
			return keys[(uint64(i)*2654435761)&(entries-1)]
		}
		b.Run(name+"/get", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				value, closer, err := db.Get(keyAt(i))
				if err != nil {
					b.Fatal(err)
				}
				benchmarkSeekPrefixValue = value
				if err := closer.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
		for _, seek := range []struct {
			name string
			fn   func(*pebble.Iterator, []byte) bool
		}{
			{name: "seek-ge", fn: func(iter *pebble.Iterator, key []byte) bool { return iter.SeekGE(key) }},
			{name: "seek-prefix-ge", fn: func(iter *pebble.Iterator, key []byte) bool { return iter.SeekPrefixGE(key) }},
		} {
			b.Run(name+"/"+seek.name, func(b *testing.B) {
				iter, err := db.NewIter(nil)
				if err != nil {
					b.Fatal(err)
				}
				defer iter.Close()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					key := keyAt(i)
					if !seek.fn(iter, key) || !bytes.Equal(iter.Key(), key) {
						b.Fatal("key not found")
					}
					benchmarkSeekPrefixValue = iter.Value()
				}
			})
		}
	}
}
