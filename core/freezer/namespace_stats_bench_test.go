package freezer

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// BenchmarkHotBlockNamespaceSize compares the previous full-prefix scan with
// bounded sampling over the same 65,536-row Pebble namespace. Bounded results
// are intentionally lower bounds, not an equivalent estimate of total bytes.
func BenchmarkHotBlockNamespaceSize(b *testing.B) {
	const blocks = 65_536
	opts := rawdb.DefaultPebbleOptions()
	opts.MemTableSizeBytes = 8 << 20
	db, err := rawdb.NewPebbleDBWithOptions(b.TempDir(), 64, 64, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Reset()
	payload := bytes.Repeat([]byte{0x5a}, 1024)
	for number := uint64(0); number < blocks; number++ {
		block := coretypes.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: int64(number)}, WitnessSignature: payload,
		}})
		if err := rawdb.WriteBlock(batch, block); err != nil {
			b.Fatal(err)
		}
		if number%1024 == 1023 {
			if err := batch.Write(); err != nil {
				b.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := db.Compact(nil, nil); err != nil {
		b.Fatal(err)
	}
	r := &Runner{chain: &fakeChain{db: db}}
	b.Run("FullScan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := db.NewIterator(blockNamespacePrefix, nil)
			var size uint64
			for it.Next() {
				if err := r.checkStopping(); err != nil {
					b.Fatal(err)
				}
				size += uint64(len(it.Key()) + len(it.Value()))
			}
			err := it.Error()
			it.Release()
			if err != nil || size == 0 {
				b.Fatalf("full scan: bytes=%d err=%v", size, err)
			}
		}
		b.ReportMetric(blocks, "rows/op")
	})
	b.Run("BoundedSample", func(b *testing.B) {
		b.ReportAllocs()
		var rows uint64
		for i := 0; i < b.N; i++ {
			if err := r.sampleHotBlockNamespaceSize(); err != nil {
				b.Fatal(err)
			}
			sample := r.pebbleSizeSample.Load()
			if sample.complete || sample.rows > hotBlockSizeMaxRows {
				b.Fatalf("invalid bounded sample: %+v", sample)
			}
			rows += sample.rows
		}
		b.ReportMetric(float64(rows)/float64(b.N), "rows/op")
	})
}
