package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// BenchmarkHistoryAccessorAssembly includes ETL load, posting encoding, metadata
// and dictionary assembly, checksum, Sync and rename. Input preparation is
// excluded; the same file can run against the pre-P2e source via go -overlay.
func BenchmarkHistoryAccessorAssembly(b *testing.B) {
	for _, shape := range []struct {
		name         string
		keys, perKey int
	}{
		{"unique-32768", 32768, 1}, {"mixed-4096x32", 4096, 32}, {"hot-8x32768", 8, 32768},
	} {
		b.Run(shape.name, func(b *testing.B) {
			dir := b.TempDir()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				build, err := newStateDomainChangeV6Build(etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 1 << 20}, dir, "bench")
				if err != nil {
					b.Fatal(err)
				}
				for k := 0; k < shape.keys; k++ {
					if err := build.CollectLogicalKey([]byte(fmt.Sprintf("key-%08d", k))); err != nil {
						b.Fatal(err)
					}
				}
				if err := build.FinishDictionary(); err != nil {
					b.Fatal(err)
				}
				for row := 0; row < shape.perKey; row++ {
					for k := 0; k < shape.keys; k++ {
						index := uint64(row*shape.keys + k)
						if err := build.CollectPosting(uint32(k), 100+index, 1000+index*100, index); err != nil {
							b.Fatal(err)
						}
					}
				}
				ref := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccessor, Path: "bench.kv", FromTxNum: 100, ToTxNum: 100 + uint64(shape.keys*shape.perKey)}
				b.StartTimer()
				result, _, err := build.BuildAccessor(dir, ref, uint64(shape.keys*shape.perKey))
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				build.Close()
				if err := os.Remove(filepath.Join(dir, result.Path)); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
