package rawdb

import (
	"context"
	"testing"
)

// The same authenticated shared packs and serial chunk cache used by the
// online index stage. Each scan creates and releases its own cache, as an
// index-stage invocation does. Fixture construction is outside measurement.
func BenchmarkStateHistoryMigrationScan(b *testing.B) {
	for _, shape := range []string{"Unique", "HighReuseRaw", "HighReuseSnappy"} {
		b.Run(shape, func(b *testing.B) {
			db, from, to, total := ownedReadBenchmarkFixture(b, shape)
			view, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				b.Fatal(err)
			}
			defer release()
			b.SetBytes(int64(total))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cached, cache, err := AcquireSerialStateHistoryChunkCacheView(context.Background(), view)
				if err != nil {
					b.Fatal(err)
				}
				rows := 0
				err = IterateStateDomainChangesByBlockTxRangeBorrowed(cached, from, to, from, to, func(change *StateDomainChange) (bool, error) {
					rows++
					if change.BlockNum != from+uint64(rows-1) || len(change.Prev) != 512<<10 {
						b.Fatal("changed scan input")
					}
					return true, nil
				})
				closeErr := cache.Close()
				if closeErr != nil {
					b.Fatal(closeErr)
				}
				if err != nil || rows != int(to-from+1) {
					b.Fatalf("rows=%d err=%v", rows, err)
				}
			}
		})
	}
}
