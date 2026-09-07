package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// These are deterministic synthetic logical history streams, not estimates of
// the mainnet business mix. All codecs receive the exact same bytes. The writer
// is the production factory, including its normal worker count, file sync and
// one-pass finalization. V3 uses bounded ordered anchor compression workers.
func cdcBenchmarkInputs() []struct {
	name string
	data []byte
} {
	rng := rand.New(rand.NewSource(707))
	accounts := make([]byte, 0, 8<<20)
	for i := 0; i < 65536; i++ {
		value := make([]byte, 107)
		copy(value, []byte("\x81account-native-small"))
		rng.Read(value[24:45])
		binary.LittleEndian.PutUint64(value[48:56], uint64(rng.Int63n(1_000_000_000)))
		binary.LittleEndian.PutUint64(value[72:80], uint64(i/3))
		accounts = binary.BigEndian.AppendUint32(accounts, uint32(len(value)+17))
		accounts = binary.BigEndian.AppendUint64(accounts, uint64(i+1))
		accounts = binary.BigEndian.AppendUint64(accounts, uint64(i))
		accounts = append(accounts, 1)
		accounts = append(accounts, value...)
	}
	random := make([]byte, 8<<20)
	rng.Read(random)
	return []struct {
		name string
		data []byte
	}{
		{"RepeatedLargeLists", cdcRepeatedInput(2<<20, 8)},
		{"SmallAccounts", accounts},
		{"LowRepeatRandom", random},
	}
}

func BenchmarkHistoryCompressionFactory(b *testing.B) {
	workers := historyCompressionConcurrency(runtime.GOMAXPROCS(0))
	for _, input := range cdcBenchmarkInputs() {
		b.Run(input.name, func(b *testing.B) {
			for _, format := range []string{"2", "3"} {
				b.Run("V"+format, func(b *testing.B) {
					dir := b.TempDir()
					path := filepath.Join(dir, "history.seg")
					write := func() {
						w, err := newHistoryCompressedStreamFormat(context.Background(), dir, historyCompressChunkSize, workers, format)
						if err != nil {
							b.Fatal(err)
						}
						defer w.Abort()
						if _, err := w.Write(input.data); err != nil {
							b.Fatal(err)
						}
						if _, err := w.FinishWithMetadataContext(context.Background(), path); err != nil {
							b.Fatal(err)
						}
					}
					write()
					info, err := os.Stat(path)
					if err != nil {
						b.Fatal(err)
					}
					check, err := openCompressedBlockReader(path)
					if err != nil {
						b.Fatal(err)
					}
					decoded := make([]byte, len(input.data))
					if _, err := check.ReadAt(decoded, 0); err != nil {
						b.Fatal(err)
					}
					if !bytes.Equal(decoded, input.data) {
						b.Fatal("round-trip differs")
					}
					check.Close()
					metrics := func(b *testing.B) {
						b.ReportMetric(float64(info.Size()), "encoded-B")
						b.ReportMetric(float64(workers), "factory-workers")
					}
					b.Run("Encode", func(b *testing.B) {
						b.SetBytes(int64(len(input.data)))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							write()
						}
						b.StopTimer()
						metrics(b)
					})
					b.Run("Open", func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							r, err := openCompressedBlockReader(path)
							if err != nil {
								b.Fatal(err)
							}
							if err := r.Close(); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						metrics(b)
					})
					b.Run("Decode", func(b *testing.B) {
						r, err := openCompressedBlockReader(path)
						if err != nil {
							b.Fatal(err)
						}
						defer r.Close()
						dst := make([]byte, len(input.data))
						b.SetBytes(int64(len(dst)))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if _, err := r.ReadAt(dst, 0); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						metrics(b)
					})
					b.Run("Random4KiB", func(b *testing.B) {
						r, err := openCompressedBlockReader(path)
						if err != nil {
							b.Fatal(err)
						}
						defer r.Close()
						dst := make([]byte, 4096)
						b.SetBytes(4096)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							off := int64((uint64(i) * 2654435761) % uint64(len(input.data)-len(dst)))
							if _, err := r.ReadAt(dst, off); err != nil {
								b.Fatal(fmt.Errorf("offset %d: %w", off, err))
							}
						}
						b.StopTimer()
						metrics(b)
					})
				})
			}
		})
	}
}
