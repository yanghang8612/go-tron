package domains

import (
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// BenchmarkOrderedCommitmentMultiBlock measures the production 64-partition
// constructor, Submit, layer commit, and durable flush on four changing blocks.
// Key selection and updates are built outside the timer. The fixed partition
// distributions expose both sparse and uneven branch-map reservations.
func BenchmarkOrderedCommitmentMultiBlock(b *testing.B) {
	for _, tc := range []struct {
		name   string
		counts [64]int
	}{
		{name: "one-partition", counts: func() (v [64]int) { v[0] = 256; return }()},
		{name: "two-partitions", counts: func() (v [64]int) { v[0], v[1] = 128, 128; return }()},
		{name: "uniform-64", counts: func() (v [64]int) {
			for i := range v {
				v[i] = 8
			}
			return
		}()},
		{name: "skewed-first-64", counts: func() (v [64]int) {
			for i := range v {
				v[i] = 8
			}
			v[0] = 256
			return
		}()},
		{name: "skewed-last-64", counts: func() (v [64]int) {
			for i := range v {
				v[i] = 8
			}
			v[63] = 256
			return
		}()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Setenv("GTRON_COMMITMENT_PARTITIONS", "64")
			keys := make([][]byte, 0, 1024)
			remaining := tc.counts
			want := 0
			for _, n := range remaining {
				want += n
			}
			for candidate := uint64(1); len(keys) < want; candidate++ {
				var key [8]byte
				binary.BigEndian.PutUint64(key[:], candidate)
				partition := commitmentPartition(keyPath(key[:]))
				if remaining[partition] == 0 {
					continue
				}
				keys = append(keys, append([]byte(nil), key[:]...))
				remaining[partition]--
			}
			updates := make([][]rawdb.StateCommitmentUpdate, 4)
			for block := range updates {
				updates[block] = make([]rawdb.StateCommitmentUpdate, len(keys))
				for i, key := range keys {
					updates[block][i] = rawdb.NewStateCommitmentPut(key, []byte{byte(block + 1), byte(i), byte(i >> 8)})
				}
			}
			disk := rawdb.NewMemoryDatabase()
			buffer := blockbuffer.New(disk)
			pipeline, err := NewOrderedCommitmentPipeline(buffer)
			if err != nil {
				b.Fatal(err)
			}
			defer pipeline.Close()
			var number uint64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for block := range updates {
					number++
					buffer.BeginBlock(common.Hash{byte(number), byte(number >> 8)}, number)
					h, ok := buffer.NewestInflight()
					if !ok {
						b.Fatal("missing in-flight layer")
					}
					result := <-pipeline.Submit(buffer.ViewLayer(h), updates[block])
					if result.Err != nil {
						b.Fatal(result.Err)
					}
					if err := buffer.CommitInflight(h); err != nil {
						b.Fatal(err)
					}
				}
				if err := buffer.Flush(disk); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
