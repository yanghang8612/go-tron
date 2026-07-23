package blockbuffer

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func benchmarkLayerLookup(b *testing.B, parallel bool, hit bool) {
	l := newLayer([32]byte{}, 1)
	writer := &Buffer{}
	keys := make([][]byte, 256)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("state-commitment-branch-v1-%064x", i))
		if hit {
			writer.putInto(l, keys[i], []byte("encoded-branch"))
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	if !parallel {
		for i := 0; i < b.N; i++ {
			l.lookup(keys[i&(len(keys)-1)])
		}
		return
	}
	var next atomic.Uint64
	b.SetParallelism(1)
	b.RunParallel(func(pb *testing.PB) {
		key := keys[int(next.Add(1)-1)&(len(keys)-1)]
		for pb.Next() {
			l.lookup(key)
		}
	})
}

func BenchmarkLayerLookupSerialMiss(b *testing.B)   { benchmarkLayerLookup(b, false, false) }
func BenchmarkLayerLookupParallelMiss(b *testing.B) { benchmarkLayerLookup(b, true, false) }
func BenchmarkLayerLookupSerialHit(b *testing.B)    { benchmarkLayerLookup(b, false, true) }
func BenchmarkLayerLookupParallelHit(b *testing.B)  { benchmarkLayerLookup(b, true, true) }

func benchmarkBufferGetNoCopy(b *testing.B, parallel bool, hit bool) {
	buf := New(rawdb.NewMemoryDatabase())
	for layerN := 0; layerN < 4; layerN++ {
		buf.BeginBlock([32]byte{byte(layerN + 1)}, uint64(layerN+1))
		for keyN := 0; keyN < 256; keyN++ {
			key := []byte(fmt.Sprintf("state-commitment-branch-v1-%02x-%064x", layerN, keyN))
			if err := buf.Put(key, []byte("encoded-branch")); err != nil {
				b.Fatal(err)
			}
		}
		buf.CommitBlock()
	}
	keys := make([][]byte, 256)
	for i := range keys {
		if hit {
			keys[i] = []byte(fmt.Sprintf("state-commitment-branch-v1-03-%064x", i))
		} else {
			keys[i] = []byte(fmt.Sprintf("state-commitment-branch-v1-missing-%064x", i))
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	if !parallel {
		for i := 0; i < b.N; i++ {
			_, _ = buf.GetNoCopy(keys[i&(len(keys)-1)])
		}
		return
	}
	var next atomic.Uint64
	b.SetParallelism(1)
	b.RunParallel(func(pb *testing.PB) {
		key := keys[int(next.Add(1)-1)&(len(keys)-1)]
		for pb.Next() {
			_, _ = buf.GetNoCopy(key)
		}
	})
}

func BenchmarkBufferGetNoCopySerialMiss(b *testing.B)   { benchmarkBufferGetNoCopy(b, false, false) }
func BenchmarkBufferGetNoCopyParallelMiss(b *testing.B) { benchmarkBufferGetNoCopy(b, true, false) }
func BenchmarkBufferGetNoCopySerialHit(b *testing.B)    { benchmarkBufferGetNoCopy(b, false, true) }
func BenchmarkBufferGetNoCopyParallelHit(b *testing.B)  { benchmarkBufferGetNoCopy(b, true, true) }
