package state

import (
	"fmt"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// Every iteration reads the same rooted input. The two large rows bypass the
// decoded cache's unchanged 32 MiB limit, as in the live repeated-decode profile.
// Include the journal/revert and production history flush paths, rather than
// measuring only slice assembly. The backing database and history sink are RAM.
func BenchmarkDelegationPagedLoadMutation(b *testing.B) {
	for _, degree := range []int{32, 300_000, 1_000_000} {
		b.Run(fmt.Sprintf("degree=%d", degree), func(b *testing.B) {
			f := seedLegacyBenchmark(b, degree)
			for _, workload := range []string{"Duplicate", "AppendRevert", "RemoveReaddRevert", "HistoryFlush"} {
				b.Run(workload, func(b *testing.B) {
					sink := ethrawdb.NewMemoryDatabase()
					defer sink.Close()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s := f.open(b)
						snapshot := s.Snapshot()
						var err error
						switch workload {
						case "Duplicate":
							err = s.WriteDrAccountIndexLegacyDelegate(f.from, f.existing)
						case "AppendRevert":
							err = s.WriteDrAccountIndexLegacyDelegate(f.from, f.fresh)
						case "RemoveReaddRevert":
							err = s.WriteDrAccountIndexLegacyUnDelegate(f.from, f.existing)
							if err == nil {
								err = s.WriteDrAccountIndexLegacyDelegate(f.from, f.existing)
							}
						case "HistoryFlush":
							s.BeginDomainChangeJournalCapture(sink, 2, tcommon.Hash{2}, 2, 3)
							mark := s.DomainChangeJournalMark()
							err = s.WriteDrAccountIndexLegacyUnDelegate(f.from, f.existing)
							if err == nil {
								s.FinalizeTransaction()
								err = s.FlushDomainChangesSince(mark, 2)
							}
							if err == nil {
								err = s.FlushPendingDomainChanges(3)
							}
						}
						if err != nil {
							b.Fatal(err)
						}
						if workload != "HistoryFlush" {
							s.RevertToSnapshot(snapshot)
						}
					}
				})
			}
		})
	}
}
