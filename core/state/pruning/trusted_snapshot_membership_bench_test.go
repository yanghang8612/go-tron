package pruning

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// BenchmarkTrustedSnapshotRecordingMixed measures the complete public operation
// against its frozen legacy oracle: production manifest ReadFile/byte check and
// mutable copy, exact membership, fresh file Stats and cache registration. The
// first-record cases include writing/fsyncing a new durable cache record;
// already-recorded cases include the repeated registration used by lifecycle.
// Setup/reset and optional cache prefill are outside the timer on both sides.
// The catalog uses observed 70,198 active / 92,667 retired kind counts, with
// synthetic names and one-byte files. Trusted recording deliberately does not
// hash contents; this benchmark is not a content-authentication or sync replay.
func BenchmarkTrustedSnapshotRecordingMixed(b *testing.B) {
	b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES", "268435456")
	manifest := mixedHistoryCompanionManifest(2794, 30908, 92667)
	dir := b.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "history"), 0700); err != nil {
		b.Fatal(err)
	}
	var histories []snapshots.SegmentRef
	var unrelated snapshots.SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Dataset == snapshots.SegmentDatasetStateDomainChange {
			if err := os.WriteFile(filepath.Join(dir, ref.Path), []byte{0}, 0600); err != nil {
				b.Fatal(err)
			}
			if ref.Kind == snapshots.SegmentHistory {
				histories = append(histories, ref)
			} else {
				unrelated = ref
			}
		}
	}
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		b.Fatal(err)
	}
	first := histories[0]
	missing := first
	missing.Path += ".absent"
	identityMismatch := first
	identityMismatch.Size++
	for _, batch := range []struct {
		name string
		refs []snapshots.SegmentRef
	}{
		{"one", []snapshots.SegmentRef{first}},
		{"three", histories[:3]},
		{"empty", nil},
		{"unrelated", []snapshots.SegmentRef{unrelated}},
		{"missing", []snapshots.SegmentRef{missing}},
		{"identity-mismatch", []snapshots.SegmentRef{identityMismatch}},
		{"duplicates", []snapshots.SegmentRef{first, first, first}},
	} {
		b.Run(batch.name, func(b *testing.B) {
			for _, state := range []string{"first-record", "already-recorded"} {
				b.Run(state, func(b *testing.B) {
					for _, mode := range []string{"legacy", "candidate"} {
						b.Run(mode, func(b *testing.B) {
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								b.StopTimer()
								if err := os.RemoveAll(filepath.Join(dir, snapshotCoverageVerificationCacheFile)); err != nil {
									b.Fatal(err)
								}
								p := trustedSnapshotPruner(dir)
								if state == "already-recorded" {
									if err := legacyRecordTrustedSnapshotSegments(p, batch.refs); err != nil {
										b.Fatal(err)
									}
								}
								b.StartTimer()
								var err error
								if mode == "legacy" {
									err = legacyRecordTrustedSnapshotSegments(p, batch.refs)
								} else {
									err = p.RecordTrustedSnapshotSegments(batch.refs)
								}
								b.StopTimer()
								if err != nil {
									b.Fatal(err)
								}
							}
							b.ReportMetric(float64(len(manifest.Segments)), "active-refs")
							b.ReportMetric(float64(len(manifest.Retired)), "retired-refs")
							b.ReportMetric(float64(len(batch.refs)), "input-refs")
						})
					}
				})
			}
		})
	}
}
