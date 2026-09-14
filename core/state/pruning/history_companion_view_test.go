package pruning

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Frozen pre-view constructor, retained only as a test/benchmark reference.
// Both versions perform the same production manifest read, fresh Stats, active
// cache retention and segment ordering. Only companion resolution differs.
func scanSnapshotCoverageGate(w Worker, ctx context.Context) (*snapshotStateDomainCoverageGate, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	gate := &snapshotStateDomainCoverageGate{ctx: ctx, dir: w.SnapshotDir, cache: w.coverageVerificationCache}
	if (w.Policy.Mode != ModeSnap && w.Policy.Mode != ModeArchive) || w.SnapshotDir == "" {
		return gate, nil
	}
	manifest, err := snapshots.LoadProductionManifest(w.SnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return gate, nil
		}
		return nil, err
	}
	gate.manifest = manifest
	active := make(map[snapshotHistoryVerificationKey]struct{})
	for _, ref := range manifest.Segments {
		if ref.NormalizedDataset() != snapshots.SegmentDatasetStateDomainChange || ref.Kind != snapshots.SegmentHistory {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key, err := snapshotHistoryVerificationKeyFor(w.SnapshotDir, manifest, ref)
		if err != nil {
			return nil, fmt.Errorf("hot prune coverage: identify state-domain history %q: %w", ref.Path, err)
		}
		active[key] = struct{}{}
		gate.segments = append(gate.segments, snapshotStateDomainCoverageSegment{ref: ref, key: key})
	}
	if w.coverageVerificationCache != nil {
		w.coverageVerificationCache.setActiveManifest(active)
		if err := w.coverageVerificationCache.retain(active); err != nil {
			return nil, fmt.Errorf("pruning: retain active state-domain history verifications: %w", err)
		}
	}
	sort.Slice(gate.segments, func(i, j int) bool {
		if gate.segments[i].ref.FromTxNum != gate.segments[j].ref.FromTxNum {
			return gate.segments[i].ref.FromTxNum < gate.segments[j].ref.FromTxNum
		}
		return gate.segments[i].ref.ToTxNum < gate.segments[j].ref.ToTxNum
	})
	return gate, nil
}

func historyCompanionWorkerFixture(t *testing.T) (Worker, *snapshots.Manifest) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	var refs []snapshots.SegmentRef
	for block := uint64(1); block <= 2; block++ {
		from, to := block*10, block*10+2
		writeSnapPruningChange(t, db, block, from, to)
		built, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, from, to, fmt.Sprintf("history/state-domain-change-%d-%d.seg", from, to))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, built...)
	}
	manifest := snapshots.NewManifest(10, 22, refs)
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	return Worker{DB: db, SnapshotDir: dir, Policy: SnapPolicy(1, 1)}, manifest
}

func TestHistoryCompanionGateMatchesScanAndFreshFileIdentity(t *testing.T) {
	for _, kind := range []snapshots.SegmentKind{snapshots.SegmentHistory, snapshots.SegmentInverted, snapshots.SegmentAccessor} {
		t.Run(string(kind), func(t *testing.T) {
			w, manifest := historyCompanionWorkerFixture(t)
			history := historyRef(t, manifest.Segments)
			view := snapshots.NewHistoryCompanionView(manifest)
			oldKey, err := snapshotHistoryVerificationKeyForView(w.SnapshotDir, manifest, history, view)
			if err != nil {
				t.Fatal(err)
			}
			before, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want, err := scanSnapshotCoverageGate(w, context.Background())
			if err != nil || !reflect.DeepEqual(before.segments, want.segments) {
				t.Fatalf("gate differs: %v", err)
			}
			ref := segmentRefByKind(t, manifest.Segments, kind)
			path := filepath.Join(w.SnapshotDir, ref.Path)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			mtime := info.ModTime().Add(2 * time.Second)
			if err := os.Chtimes(path, mtime, mtime); err != nil {
				t.Fatal(err)
			}
			fresh, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want, err = scanSnapshotCoverageGate(w, context.Background())
			if err != nil || !reflect.DeepEqual(fresh.segments, want.segments) {
				t.Fatalf("fresh gate differs: %v", err)
			}
			if reflect.DeepEqual(before.segments, fresh.segments) {
				t.Fatal("same-size mtime replacement reused stale file identity")
			}
			newKey, err := snapshotHistoryVerificationKeyForView(w.SnapshotDir, manifest, history, view)
			if err != nil || newKey == oldKey {
				t.Fatalf("reusing a view hid the fresh Stat identity: %v", err)
			}
			if err := before.verify(0); err == nil {
				t.Fatal("lazy verification accepted changed file identity")
			}
			if err := os.Truncate(path, info.Size()+1); err != nil {
				t.Fatal(err)
			}
			_, sizeErr := snapshotHistoryVerificationKeyForView(w.SnapshotDir, manifest, history, view)
			_, scanSizeErr := snapshotHistoryVerificationKeyFor(w.SnapshotDir, manifest, history)
			if sizeErr == nil || scanSizeErr == nil || sizeErr.Error() != scanSizeErr.Error() {
				t.Fatalf("size mismatch: view=%v scan=%v", sizeErr, scanSizeErr)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			_, gotErr := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
			_, wantErr := scanSnapshotCoverageGate(w, context.Background())
			if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
				t.Fatalf("missing file: view=%v scan=%v", gotErr, wantErr)
			}
		})
	}
}

func TestHistoryCompanionVerificationKeyIdentityAndNilFallback(t *testing.T) {
	w, manifest := historyCompanionWorkerFixture(t)
	history := historyRef(t, manifest.Segments)
	for _, mutation := range []string{"none", "index", "accessor"} {
		t.Run(mutation, func(t *testing.T) {
			m := *manifest
			m.Segments = append([]snapshots.SegmentRef(nil), manifest.Segments...)
			for i := range m.Segments {
				ref := &m.Segments[i]
				if ref.FromTxNum == history.FromTxNum && ((mutation == "index" && ref.Kind == snapshots.SegmentInverted) || (mutation == "accessor" && ref.Kind == snapshots.SegmentAccessor)) {
					ref.AggregationSteps = 2
				}
			}
			want, wantErr := snapshotHistoryVerificationKeyFor(w.SnapshotDir, &m, history)
			for _, view := range []*snapshots.HistoryCompanionView{nil, snapshots.NewHistoryCompanionView(&m)} {
				got, gotErr := snapshotHistoryVerificationKeyForView(w.SnapshotDir, &m, history, view)
				if got != want || (gotErr == nil) != (wantErr == nil) || (gotErr != nil && gotErr.Error() != wantErr.Error()) {
					t.Fatalf("identity mismatch: %v / %v", gotErr, wantErr)
				}
			}
			if mutation != "none" && wantErr == nil {
				t.Fatal("invalid companion accepted")
			}
		})
	}
}

func TestHistoryCompanionGateFreshManifestBytesAndCancellation(t *testing.T) {
	w, _ := historyCompanionWorkerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	gate, err := w.newSnapshotStateDomainChangeCoverageGate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := w.newSnapshotStateDomainChangeCoverageGate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("constructor cancellation: %v", err)
	}
	if err := gate.verify(0); !errors.Is(err, context.Canceled) {
		t.Fatalf("verification cancellation: %v", err)
	}
	path := filepath.Join(w.SnapshotDir, snapshots.ManifestFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// A same-size and same-mtime manifest replacement must still be read and
	// validated, even after the previous construction warmed the decode cache.
	changed := bytes.Replace(raw, []byte(`"generation":1`), []byte(`"generation":2`), 1)
	if bytes.Equal(changed, raw) {
		t.Fatal("fixture missing generation")
	}
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	fresh, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
	if err != nil || fresh.manifest.Generation != 2 {
		t.Fatalf("same-stat bytes not refreshed: %v", err)
	}
	changed[0] = '!'
	if err := os.WriteFile(path, changed, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background()); err == nil {
		t.Fatal("malformed replacement accepted")
	}
}
