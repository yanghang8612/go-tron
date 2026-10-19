package rawdb

import (
	"fmt"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"testing"
)

var compactBenchValue []byte

func BenchmarkCompactWordCodec(b *testing.B) {
	for _, width := range []int{0, 1, 4, 8, 20, 32} {
		word := make([]byte, 32)
		for i := 32 - width; i < 32; i++ {
			word[i] = byte(i + 1)
		}
		encoded := EncodeStateKVLatestValue(word)
		b.Run(fmt.Sprintf("encode/width%d", width), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				compactBenchValue = EncodeStateKVLatestValue(word)
			}
		})
		b.Run(fmt.Sprintf("decode/width%d", width), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				compactBenchValue, err = DecodeStateKVLatestValue(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func BenchmarkCompactPredecessor(b *testing.B) {
	for _, count := range []int{1, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db, err := NewPebbleDB(b.TempDir(), 16, 16)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			owner := common.Address{0x41, 7}
			batch := db.NewBatch()
			defer batch.Close()
			for h := 1; h <= count; h++ {
				key := ShieldedRootChangeStateKey(uint64(h))
				value := make([]byte, 32)
				value[31] = byte(h)
				if err := WriteStateKVLatest(batch, owner, 0, kvdomains.SystemShielded, key, value); err != nil {
					b.Fatal(err)
				}
			}
			if err := batch.Write(); err != nil {
				b.Fatal(err)
			}
			prefix := ShieldedRootChangePrefix()
			upper := ShieldedRootChangeStateKey(uint64(count + 1))
			// A single oversized setup batch may lazily sort its flushable index
			// on the first iterator. Exclude that one-time setup from steady queries.
			if _, _, ok, err := ReadStateKVLatestBefore(db, owner, 0, kvdomains.SystemShielded, prefix, upper); err != nil || !ok {
				b.Fatal(ok, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, v, ok, err := ReadStateKVLatestBefore(db, owner, 0, kvdomains.SystemShielded, prefix, upper)
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
				compactBenchValue = v
			}
			// Closing Pebble flushes the setup batch; durability work is not a lookup.
			b.StopTimer()
		})
	}
}
