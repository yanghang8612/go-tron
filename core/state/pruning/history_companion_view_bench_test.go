package pruning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Exact active kind counts and retired count from the pinned 2026-09-14 native
// manifest (generation 33683). The synthetic names, ranges, sizes and checksums
// are representative metadata, not copies of live segment content.
func mixedHistoryCompanionManifest(histories, events, retired int) *snapshots.Manifest {
	refs := make([]snapshots.SegmentRef, 0, histories*3+events*2)
	checksum := "sha256:" + strings.Repeat("b", 64)
	for i := 0; i < histories; i++ {
		from, to := uint64(i*10), uint64(i*10+9)
		for j, kind := range []snapshots.SegmentKind{snapshots.SegmentHistory, snapshots.SegmentInverted, snapshots.SegmentAccessor} {
			refs = append(refs, snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: kind, FromTxNum: from, ToTxNum: to,
				Path: fmt.Sprintf("history/state-domain-change-%d-%d.%s", from, to, []string{"seg", "idx", "kv"}[j]), Size: 1, Checksum: checksum})
		}
	}
	for i := 0; i < events; i++ {
		from, to := uint64(i*10), uint64(i*10+9)
		for j, kind := range []snapshots.SegmentKind{snapshots.SegmentEventLog, snapshots.SegmentEventLogIndex} {
			refs = append(refs, snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetEventLog, Kind: kind, FromTxNum: from, ToTxNum: to,
				Path: fmt.Sprintf("log/event-%d.%s", i, []string{"seg", "idx"}[j]), Size: 1, Checksum: checksum})
		}
	}
	m := snapshots.NewManifest(0, uint64(histories*10-1), refs)
	m.Generation, m.PublishedUnix = 33683, 1789370000
	m.Retired = make([]snapshots.SegmentRef, retired)
	for i := range m.Retired {
		m.Retired[i] = refs[i%len(refs)]
		m.Retired[i].Path = fmt.Sprintf("retired/%d-%s", i, filepath.Base(m.Retired[i].Path))
	}
	return m
}

func benchmarkHistoryCompanionCounts(b *testing.B, m *snapshots.Manifest) {
	b.Helper()
	b.ReportMetric(float64(len(m.Segments)), "active-refs")
	b.ReportMetric(float64(len(m.Retired)), "retired-refs")
	// These are the view's extra backing-container bytes, excluding small
	// structs/allocator rounding. Strings are shared; no retired copy is made.
	b.ReportMetric(float64(len(m.Segments))*float64(unsafe.Sizeof(snapshots.SegmentRef{})), "view-copy-B")
	b.ReportMetric(float64(len(m.Segments)*4), "view-path-index-B")
}

// BenchmarkHistoryCompanionGateMixed includes the actual gate constructor:
// current JSON ReadFile/byte check, detached public loader copy, companion view
// construction, three fresh file Stats per history, active cache retain, and
// gate sort. Files are one-byte placeholders because construction authenticates
// metadata identities only; SHA/content verification remains a later operation.
func BenchmarkHistoryCompanionGateMixed(b *testing.B) {
	b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES", "268435456")
	m := mixedHistoryCompanionManifest(2794, 30908, 92667)
	dir := b.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0700); err != nil {
		b.Fatal(err)
	}
	for _, ref := range m.Segments {
		if ref.Dataset == snapshots.SegmentDatasetStateDomainChange {
			if err := os.WriteFile(filepath.Join(dir, ref.Path), []byte{0}, 0600); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := snapshots.PublishManifest(dir, m); err != nil {
		b.Fatal(err)
	}
	w := Worker{SnapshotDir: dir, Policy: SnapPolicy(1, 1), coverageVerificationCache: newSnapshotCoverageVerificationCache(dir)}
	for _, mode := range []string{"scan", "view"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var gate *snapshotStateDomainCoverageGate
				var err error
				if mode == "scan" {
					gate, err = scanSnapshotCoverageGate(w, context.Background())
				} else {
					gate, err = w.newSnapshotStateDomainChangeCoverageGate(context.Background())
				}
				if err != nil || len(gate.segments) != 2794 {
					b.Fatalf("gate result: %v", err)
				}
			}
			b.StopTimer()
			benchmarkHistoryCompanionCounts(b, m)
		})
	}
}

// BenchmarkHistoryCompanionMetadataReplay never touches segment files. With no
// env it uses the mixed synthetic catalog. For a frozen native JSON, set BOTH
// GTRON_HISTORY_COMPANION_MANIFEST and GTRON_HISTORY_COMPANION_MANIFEST_SHA256.
// JSON read/decode/validation run before the timer; each timed view iteration
// includes the detached active copy, index build and both companion queries for
// every state history. This is a component benchmark, not sync throughput.
func BenchmarkHistoryCompanionMetadataReplay(b *testing.B) {
	var m *snapshots.Manifest
	if path := os.Getenv("GTRON_HISTORY_COMPANION_MANIFEST"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		want := os.Getenv("GTRON_HISTORY_COMPANION_MANIFEST_SHA256")
		if want == "" || want != hex.EncodeToString(sum[:]) {
			b.Fatal("frozen manifest SHA256 missing or mismatched")
		}
		m = new(snapshots.Manifest)
		if err := json.Unmarshal(raw, m); err != nil {
			b.Fatal(err)
		}
		b.Logf("frozen metadata SHA256=%s bytes=%d generation=%d", want, len(raw), m.Generation)
	} else {
		m = mixedHistoryCompanionManifest(2794, 30908, 92667)
	}
	if err := m.Validate(); err != nil {
		b.Fatal(err)
	}
	if err := m.ValidateProduction(); err != nil {
		b.Fatal(err)
	}
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	var histories []snapshots.SegmentRef
	for _, ref := range m.Segments {
		if ref.NormalizedDataset() == snapshots.SegmentDatasetStateDomainChange && ref.Kind == snapshots.SegmentHistory {
			histories = append(histories, ref)
		}
	}
	if len(histories) == 0 {
		b.Fatal("manifest contains no state-domain history")
	}
	for _, mode := range []string{"scan", "view"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var view *snapshots.HistoryCompanionView
				if mode == "view" {
					view = snapshots.NewHistoryCompanionView(m)
					if view == nil {
						b.Fatal("manifest exceeds bounded view limit")
					}
				}
				for _, history := range histories {
					var index, accessor snapshots.SegmentRef
					var indexOK, accessorOK bool
					if view == nil {
						index, indexOK = cfg.HistoryIndexRef(m, history)
						accessor, accessorOK = cfg.HistoryAccessorRef(m, history)
					} else {
						index, indexOK = view.HistoryIndexRef(cfg, history)
						accessor, accessorOK = view.HistoryAccessorRef(cfg, history)
					}
					if !indexOK || !accessorOK || index.FromTxNum != history.FromTxNum || accessor.ToTxNum != history.ToTxNum {
						b.Fatal("missing or mismatched companion")
					}
				}
			}
			b.StopTimer()
			benchmarkHistoryCompanionCounts(b, m)
			b.ReportMetric(float64(len(histories)), "histories")
		})
	}
}
