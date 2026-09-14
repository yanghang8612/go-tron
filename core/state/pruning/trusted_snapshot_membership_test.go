package pruning

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Frozen full pre-optimization operation. Keep manifest loading, lookup,
// file identities, cache publication, duplicate accounting and first-error
// behavior in the oracle, rather than comparing membership in isolation.
func legacyRecordTrustedSnapshotSegments(p *Pruner, refs []snapshots.SegmentRef) error {
	if p == nil || p.coverageVerificationCache == nil || p.cfg.SnapshotDir == "" || len(refs) == 0 {
		return nil
	}
	manifest, err := snapshots.LoadProductionManifest(p.cfg.SnapshotDir)
	if err != nil {
		return err
	}
	active := make(map[snapshots.SegmentRef]struct{}, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		active[ref] = struct{}{}
	}
	for _, ref := range refs {
		if ref.NormalizedDataset() != snapshots.SegmentDatasetStateDomainChange || ref.Kind != snapshots.SegmentHistory {
			continue
		}
		if _, ok := active[ref]; !ok {
			continue
		}
		key, err := snapshotHistoryVerificationKeyFor(p.cfg.SnapshotDir, manifest, ref)
		if err != nil {
			return fmt.Errorf("pruning: record trusted state-domain history %q: %w", ref.Path, err)
		}
		if err := p.coverageVerificationCache.addTrusted(key); err != nil {
			return fmt.Errorf("pruning: persist trusted state-domain history %q: %w", ref.Path, err)
		}
	}
	return nil
}

type trustedSnapshotOutcome struct {
	err        string
	stats      snapshotCoverageVerificationCacheStats
	verified   map[snapshotHistoryVerificationKey]struct{}
	persistent map[snapshotHistoryVerificationRecord]struct{}
	observed   []snapshotCoverageVerificationCacheStats
	disk       []byte
	diskErr    string
}

func trustedSnapshotOutcomeFor(p *Pruner, err error, observed []snapshotCoverageVerificationCacheStats) trustedSnapshotOutcome {
	out := trustedSnapshotOutcome{observed: observed}
	if err != nil {
		out.err = err.Error()
		// CreateTemp chooses a fresh source name for each persistence attempt.
		// Normalize only that known rename source; preserve the error wrapper,
		// operation, destination and underlying OS error in the full oracle.
		var rename *os.LinkError
		if errors.As(err, &rename) && rename.Op == "rename" &&
			rename.New == filepath.Join(p.cfg.SnapshotDir, snapshotCoverageVerificationCacheFile) &&
			filepath.Dir(rename.Old) == p.cfg.SnapshotDir &&
			strings.HasPrefix(filepath.Base(rename.Old), ".state-domain-history-verification-") &&
			strings.HasSuffix(rename.Old, ".tmp") {
			out.err = strings.Replace(out.err, rename.Old, filepath.Join(p.cfg.SnapshotDir, "<temporary-verification-cache>"), 1)
		}
	}
	cache := p.coverageVerificationCache
	out.stats = cache.Stats()
	out.verified = make(map[snapshotHistoryVerificationKey]struct{}, len(cache.verified))
	for key := range cache.verified {
		out.verified[key] = struct{}{}
	}
	out.persistent = make(map[snapshotHistoryVerificationRecord]struct{}, len(cache.persistent))
	for key := range cache.persistent {
		out.persistent[key] = struct{}{}
	}
	out.disk, err = os.ReadFile(filepath.Join(p.cfg.SnapshotDir, snapshotCoverageVerificationCacheFile))
	if err != nil {
		out.diskErr = err.Error()
	}
	return out
}

func trustedSnapshotPruner(dir string) *Pruner {
	return &Pruner{cfg: PrunerConfig{SnapshotDir: dir}, coverageVerificationCache: newSnapshotCoverageVerificationCache(dir)}
}

func TestTrustedSnapshotFullOperationMatchesLegacy(t *testing.T) {
	for _, name := range []string{
		"empty", "unrelated", "one", "two-reversed", "duplicates", "large-duplicates",
		"missing-ref", "path", "size", "checksum", "from", "to", "steps", "effective-steps", "domain", "dataset", "kind",
		"missing-first-file", "missing-second-history", "missing-second-index", "missing-second-accessor", "second-size-mismatch",
		"empty-malformed-manifest", "unrelated-malformed-manifest", "missing-manifest", "persist-failure",
	} {
		t.Run(name, func(t *testing.T) {
			w, manifest := historyCompanionWorkerFixture(t)
			var histories []snapshots.SegmentRef
			for _, ref := range manifest.Segments {
				if ref.Kind == snapshots.SegmentHistory {
					histories = append(histories, ref)
				}
			}
			first, second := histories[0], histories[1]
			unrelated := segmentRefByKind(t, manifest.Segments, snapshots.SegmentInverted)
			refs := []snapshots.SegmentRef{first}
			wantRecorded := uint64(1)
			wantError := false
			switch name {
			case "empty", "empty-malformed-manifest":
				refs, wantRecorded = nil, 0
			case "unrelated", "unrelated-malformed-manifest":
				refs, wantRecorded = []snapshots.SegmentRef{unrelated}, 0
			case "two-reversed":
				refs, wantRecorded = []snapshots.SegmentRef{second, unrelated, first}, 2
			case "duplicates":
				refs, wantRecorded = []snapshots.SegmentRef{second, first, second}, 3
			case "large-duplicates":
				refs = make([]snapshots.SegmentRef, len(manifest.Segments)+1)
				for i := range refs {
					refs[i] = first
				}
				wantRecorded = uint64(len(refs))
			case "missing-ref", "path", "size", "checksum", "from", "to", "steps", "effective-steps", "domain", "dataset", "kind":
				changed := second
				switch name {
				case "missing-ref", "path":
					changed.Path += ".missing"
				case "size":
					changed.Size++
				case "checksum":
					changed.Checksum += "different"
				case "from":
					changed.FromTxNum++
				case "to":
					changed.ToTxNum++
				case "steps":
					changed.AggregationSteps++
				case "effective-steps":
					if changed.AggregationSteps != 1 {
						t.Fatal("fixture must have explicit single-step identity")
					}
					changed.AggregationSteps = 0
				case "domain":
					changed.Domain++
				case "dataset":
					changed.Dataset = snapshots.SegmentDatasetCode
				case "kind":
					changed.Kind = snapshots.SegmentInverted
				}
				// An inactive candidate is skipped without Stat, then the valid
				// input still publishes. Even effective-steps-equivalent refs
				// (0 versus 1) require exact original SegmentRef equality.
				refs = []snapshots.SegmentRef{changed, first}
			case "missing-first-file":
				refs, wantRecorded, wantError = []snapshots.SegmentRef{second, first}, 0, true
				if err := os.Remove(filepath.Join(w.SnapshotDir, second.Path)); err != nil {
					t.Fatal(err)
				}
			case "missing-second-history", "missing-second-index", "missing-second-accessor", "second-size-mismatch":
				refs, wantRecorded, wantError = []snapshots.SegmentRef{first, first, second, first}, 2, true
				kind := snapshots.SegmentHistory
				if name == "missing-second-index" {
					kind = snapshots.SegmentInverted
				} else if name == "missing-second-accessor" {
					kind = snapshots.SegmentAccessor
				}
				for _, ref := range manifest.Segments {
					if ref.Kind == kind && ref.FromTxNum == second.FromTxNum {
						path := filepath.Join(w.SnapshotDir, ref.Path)
						if name == "second-size-mismatch" {
							if err := os.WriteFile(path, []byte{1}, 0600); err != nil {
								t.Fatal(err)
							}
						} else if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
					}
				}
			case "missing-manifest":
				wantRecorded, wantError = 0, true
				if err := os.Remove(filepath.Join(w.SnapshotDir, snapshots.ManifestFile)); err != nil {
					t.Fatal(err)
				}
			case "persist-failure":
				wantError = true
			}
			if name == "empty-malformed-manifest" || name == "unrelated-malformed-manifest" {
				if err := os.WriteFile(filepath.Join(w.SnapshotDir, snapshots.ManifestFile), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
				wantError = name == "unrelated-malformed-manifest"
			}
			var want trustedSnapshotOutcome
			for _, legacy := range []bool{true, false} {
				cachePath := filepath.Join(w.SnapshotDir, snapshotCoverageVerificationCacheFile)
				if err := os.RemoveAll(cachePath); err != nil {
					t.Fatal(err)
				}
				if name == "persist-failure" {
					if err := os.Mkdir(cachePath, 0700); err != nil {
						t.Fatal(err)
					}
				}
				p := trustedSnapshotPruner(w.SnapshotDir)
				var observed []snapshotCoverageVerificationCacheStats
				p.coverageVerificationCache.setStatsObserver(func(s snapshotCoverageVerificationCacheStats) { observed = append(observed, s) })
				var err error
				if legacy {
					err = legacyRecordTrustedSnapshotSegments(p, refs)
				} else {
					err = p.RecordTrustedSnapshotSegments(refs)
				}
				got := trustedSnapshotOutcomeFor(p, err, observed)
				if got.stats.TrustedRecorded != wantRecorded || (err != nil) != wantError {
					t.Fatalf("legacy=%v recorded=%d err=%v, want recorded=%d error=%v", legacy, got.stats.TrustedRecorded, err, wantRecorded, wantError)
				}
				if legacy {
					want = got
				} else if !reflect.DeepEqual(got, want) {
					t.Fatalf("full operation differs:\ngot %+v\nwant %+v", got, want)
				}
			}
		})
	}
}

func TestTrustedSnapshotFreshStatAcrossCallsMatchesLegacy(t *testing.T) {
	w, manifest := historyCompanionWorkerFixture(t)
	history := historyRef(t, manifest.Segments)
	path := filepath.Join(w.SnapshotDir, history.Path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var want trustedSnapshotOutcome
	for _, legacy := range []bool{true, false} {
		if err := os.RemoveAll(filepath.Join(w.SnapshotDir, snapshotCoverageVerificationCacheFile)); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
			t.Fatal(err)
		}
		p := trustedSnapshotPruner(w.SnapshotDir)
		record := p.RecordTrustedSnapshotSegments
		if legacy {
			record = func(refs []snapshots.SegmentRef) error { return legacyRecordTrustedSnapshotSegments(p, refs) }
		}
		if err := record([]snapshots.SegmentRef{history}); err != nil {
			t.Fatal(err)
		}
		newTime := info.ModTime().Add(2 * time.Second)
		if err := os.Chtimes(path, newTime, newTime); err != nil {
			t.Fatal(err)
		}
		if err := record([]snapshots.SegmentRef{history}); err != nil {
			t.Fatal(err)
		}
		got := trustedSnapshotOutcomeFor(p, nil, nil)
		if len(got.verified) != 2 || len(got.persistent) != 1 || got.stats.TrustedRecorded != 2 {
			t.Fatalf("file identity was not refreshed: %+v", got)
		}
		if legacy {
			want = got
		} else if !reflect.DeepEqual(got, want) {
			t.Fatal("fresh file identities differ from legacy operation")
		}
	}
}

func TestTrustedActiveMembershipExactAndBounded(t *testing.T) {
	first := snapshots.SegmentRef{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory, Path: "history"}
	second := first
	second.Path = "other"
	for _, active := range [][]snapshots.SegmentRef{nil, {first}, {first, first, second}} {
		for _, refs := range [][]snapshots.SegmentRef{nil, {first}, {second}, {first, first}, {first, second, first, second, first}} {
			got := activeTrustedSnapshotRefs(active, refs)
			for _, ref := range refs {
				want := false
				for _, a := range active {
					want = want || a == ref
				}
				if got.contains(ref) != want {
					t.Fatalf("membership for %+v differs", ref)
				}
			}
			if size := len(got.candidates) + len(got.catalog); size > min(len(active), len(refs)) {
				t.Fatalf("membership container not bounded: active=%d refs=%d got=%d", len(active), len(refs), size)
			}
		}
	}
}
