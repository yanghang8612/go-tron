package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
)

// This measures the same bounded codec+shared planning+atomic staging work,
// with and without reusing v2 metadata. Both paths discard the batch after each
// operation against the same immutable in-memory seed. It deliberately does
// not measure disk I/O, canonical block processing, or claim online throughput.
func BenchmarkStateHistoryChunkWork(b *testing.B) {
	for _, scenario := range []string{"repeated_seed", "repeated_existing", "single_large_no_v2", "shared_disabled"} {
		b.Run(scenario, func(b *testing.B) {
			rows := chunkHistoryRows(2<<20, 3)
			if scenario == "single_large_no_v2" {
				rows = rows[:1]
			}
			persisted := make([]persistedStateDomainChange, len(rows))
			for i, row := range rows {
				persisted[i] = persistedStateDomainChange{row.TxNum, row.FlatDomain, row.Owner, row.Generation, row.Domain, row.Key, row.PrevExists, row.Prev}
			}
			raw, err := rlp.EncodeToBytes(&persistedStateDomainChangeBlock{persistedStateDomainChangeBlockVersion, rows[0].Seq, persisted})
			if err != nil {
				b.Fatal(err)
			}
			base := NewMemoryDatabase()
			b.Cleanup(func() { _ = base.Close() })
			if scenario == "repeated_existing" {
				seed := newSharedHistoryTestBatch(base)
				baseline, _ := encodeStateDomainChangeBlockStorageWithDedup(raw, rows, true)
				_, stats, err := planAndWriteSharedStateHistory(seed, rows[0].BlockNum, raw, baseline, rows, true)
				if err != nil || stats == nil {
					b.Fatalf("seed was not admitted: %v", err)
				}
				if err := seed.batch.Write(); err != nil {
					b.Fatal(err)
				}
			}
			for _, reuse := range []bool{false, true} {
				name := "independent"
				if reuse {
					name = "reuse"
				}
				b.Run(name, func(b *testing.B) {
					b.SetBytes(int64(len(raw)))
					b.ReportAllocs()
					for b.Loop() {
						scope := newSharedHistoryTestBatch(base)
						var work stateChangeChunkWork
						var request *stateChangeChunkWork
						shared := scenario != "shared_disabled"
						if reuse && shared {
							request = &work
						}
						baseline, _ := encodeStateDomainChangeBlockStorageWithChunkWork(raw, rows, true, request)
						pack, _, err := planAndWriteSharedStateHistoryWithChunkWork(scope, rows[0].BlockNum, raw, baseline, rows, shared, request)
						if err != nil {
							b.Fatal(err)
						}
						if err := scope.Put(stateChangeSetKey(rows[0].BlockNum, 0), pack); err != nil {
							b.Fatal(err)
						}
						work = stateChangeChunkWork{}
						scope.batch.Reset()
					}
				})
			}
		})
	}
}
