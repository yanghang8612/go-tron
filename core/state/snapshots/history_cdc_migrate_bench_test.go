package snapshots

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func cdcMigrationBenchmarkSource(b *testing.B) ([]SegmentRef, map[string][]byte, uint64) {
	b.Helper()
	dir := b.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	const count = 20000
	rows := make([]*rawdb.StateDomainChange, count)
	random := rand.New(rand.NewSource(20261011))
	for i := range rows {
		value := make([]byte, 256)
		random.Read(value)
		rows[i] = &rawdb.StateDomainChange{BlockNum: 1, BlockHash: common.Hash{1}, TxNum: uint64(i + 1), Seq: uint64(i + 1), FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDelegation, Key: []byte(fmt.Sprintf("benchmark-%d", i%128)), PrevExists: true, Prev: value}
	}
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, count); err != nil {
		b.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, rows); err != nil {
		b.Fatal(err)
	}
	refs, err := BuildDiagnosticStateHistoryTrioContext(context.Background(), db, dir, 1, count, 1, 1, "history/state-domain-change-1-20000.seg", "2")
	if err != nil {
		b.Fatal(err)
	}
	refs, _, err = ReencodeHistoryReferenceTrioContext(context.Background(), dir, refs)
	if err != nil {
		b.Fatal(err)
	}
	data := make(map[string][]byte, 3)
	for _, ref := range refs {
		data[ref.Path], err = os.ReadFile(filepath.Join(dir, ref.Path))
		if err != nil {
			b.Fatal(err)
		}
	}
	h, _, _, _, _ := historyReferenceTranscodeIdentity(refs)
	r, logical, _, err := openHistorySegmentForRead(dir, h)
	if err != nil {
		b.Fatal(err)
	}
	r.Close()
	return refs, data, logical
}

func BenchmarkHistoryCDCMigrate(b *testing.B) {
	refs, source, logical := cdcMigrationBenchmarkSource(b)
	b.ReportAllocs()
	b.SetBytes(int64(logical))
	var sourceBytes uint64
	for _, ref := range refs {
		sourceBytes += ref.Size
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dir := b.TempDir()
		for name, data := range source {
			path := filepath.Join(dir, name)
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				b.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				b.Fatal(err)
			}
		}
		if err := PublishManifest(dir, NewManifest(1, 20000, refs)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := MigrateHistoryCDCContext(context.Background(), dir, HistoryCDCMigrationOptions{R1Only: true, AllowGrowth: true})
		b.StopTimer()
		if err != nil || result.CompletedTrios != 1 || result.DeletedSourceFiles != 3 {
			b.Fatal(result, err)
		}
		manifest, err := LoadProductionManifest(dir)
		if err != nil {
			b.Fatal(err)
		}
		h, _, _, _, _ := historyReferenceTranscodeIdentity(manifest.Segments)
		current, err := historyCDCIsCurrent(dir, h)
		if err != nil || !current {
			b.Fatal("not CDC", err)
		}
		var outputBytes uint64
		for _, ref := range manifest.Segments {
			outputBytes += ref.Size
		}
		b.ReportMetric(float64(h.Size), "history-B/op")
		b.ReportMetric(float64(outputBytes), "trio-B/op")
		b.ReportMetric(float64(sourceBytes), "source-trio-B/op")
		b.ReportMetric(float64(logical), "logical-B/op")
	}
}
