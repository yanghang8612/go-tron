package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
)

// Both operations run the complete shared planner, all existing reads and
// authentication, and final pack staging against the same immutable memory DB.
// planner_only excludes the pre-existing RLP/baseline codec; codec_and_planner
// includes baseline/v2 construction and per-call hash metadata. Neither includes
// disk I/O, canonical execution, RLP creation, batch commit or reopening.
func BenchmarkSharedHistoryAuthenticatedPlanner(b *testing.B) {
	for _, scenario := range []string{"repeated_seed", "repeated_existing", "single_large_existing_no_v2", "shared_disabled"} {
		b.Run(scenario, func(b *testing.B) {
			rows := chunkHistoryRows(2<<20, 3)
			if scenario == "single_large_existing_no_v2" {
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
			shared := scenario != "shared_disabled"
			var preparedWork stateChangeChunkWork
			var request *stateChangeChunkWork
			if shared {
				request = &preparedWork
			}
			baseline, _ := encodeStateDomainChangeBlockStorageWithChunkWork(raw, rows, true, request)
			base := NewMemoryDatabase()
			b.Cleanup(func() { _ = base.Close() })
			if scenario == "repeated_existing" || scenario == "single_large_existing_no_v2" {
				scope := newSharedHistoryTestBatch(base)
				_, stats, err := legacySharedAuthenticatedChunkPlanner(scope, rows[0].BlockNum, raw, baseline, rows, true, request)
				if err != nil || stats == nil {
					b.Fatalf("seed not admitted: %v", err)
				}
				if err := scope.batch.Write(); err != nil {
					b.Fatal(err)
				}
			}
			for _, operation := range []string{"planner_only", "codec_and_planner"} {
				for _, candidate := range []bool{false, true} {
					name, planner := "legacy", sharedAuthenticatedPlanner(legacySharedAuthenticatedChunkPlanner)
					if candidate {
						name, planner = "candidate", planAndWriteSharedStateHistoryWithChunkWork
					}
					b.Run(operation+"/"+name, func(b *testing.B) {
						b.SetBytes(int64(len(raw)))
						b.ReportAllocs()
						for b.Loop() {
							scope := newSharedHistoryTestBatch(base)
							codec, work := baseline, preparedWork
							var request *stateChangeChunkWork
							if shared {
								request = &work
							}
							if operation == "codec_and_planner" {
								codec, _ = encodeStateDomainChangeBlockStorageWithChunkWork(raw, rows, true, request)
							}
							pack, stats, err := planner(scope, rows[0].BlockNum, raw, codec, rows, shared, request)
							if err != nil {
								b.Fatal(err)
							}
							if err := scope.Put(stateChangeSetKey(rows[0].BlockNum, 0), pack); err != nil {
								b.Fatal(err)
							}
							stats.observe()
							work = stateChangeChunkWork{}
							scope.batch.Reset()
						}
						b.ReportMetric(float64(len(raw)), "raw_B/op")
					})
				}
			}
		})
	}
}
