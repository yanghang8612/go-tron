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

func linearCoversMetadata(segments []snapshotStateDomainCoverageSegment, from, to uint64) bool {
	next := from
	for i := range segments {
		ref := segments[i].ref
		if ref.ToTxNum < next {
			continue
		}
		if ref.FromTxNum > next {
			return false
		}
		if ref.ToTxNum >= to {
			return true
		}
		next = ref.ToTxNum + 1
	}
	return false
}

func TestHistoryCompanionGateCoversMetadataBinarySearchMatchesLinearScan(t *testing.T) {
	// Include contiguous, overlapping/nested, and gapped intervals.
	ranges := [][2]uint64{
		{0, 9},
		{5, 19},
		{10, 15},
		{20, 29},
		{20, 24},
		{35, 44},
		{40, 60},
		{61, 70},
		{80, 99},
	}
	segments := make([]snapshotStateDomainCoverageSegment, len(ranges))
	for i, r := range ranges {
		segments[i] = snapshotStateDomainCoverageSegment{
			ref: snapshots.SegmentRef{
				FromTxNum: r[0],
				ToTxNum:   r[1],
				Path:      fmt.Sprintf("seg-%d.seg", i),
			},
			verified: true,
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].ref.FromTxNum != segments[j].ref.FromTxNum {
			return segments[i].ref.FromTxNum < segments[j].ref.FromTxNum
		}
		return segments[i].ref.ToTxNum < segments[j].ref.ToTxNum
	})
	prefixMaxTo := make([]uint64, len(segments))
	var maxTo uint64
	for i := range segments {
		if segments[i].ref.ToTxNum > maxTo {
			maxTo = segments[i].ref.ToTxNum
		}
		prefixMaxTo[i] = maxTo
	}
	gate := &snapshotStateDomainCoverageGate{
		segments:    segments,
		prefixMaxTo: prefixMaxTo,
	}

	for from := uint64(0); from <= 105; from++ {
		for to := from; to <= 105; to++ {
			want := linearCoversMetadata(segments, from, to)
			gotMeta := gate.coversMetadata(from, to)
			gotFull, err := gate.covers(from, to)
			if err != nil {
				t.Fatalf("covers(%d,%d) err: %v", from, to, err)
			}
			if gotMeta != want || gotFull != want {
				t.Fatalf("range [%d,%d]: meta=%v full=%v want=%v", from, to, gotMeta, gotFull, want)
			}
		}
	}
}

func TestHistoryCompanionGateParallelStatErrorOrder(t *testing.T) {
	dir := t.TempDir()
	histDir := filepath.Join(dir, "history")
	if err := os.MkdirAll(histDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const count = 72 // >= parallelStatThreshold (64)
	refs := make([]snapshots.SegmentRef, 0, count*3)
	for i := 0; i < count; i++ {
		from := uint64(i) * 10
		to := from + 9
		base := fmt.Sprintf("history/state-domain-change-%d-%d", from, to)
		specs := []struct {
			kind snapshots.SegmentKind
			ext  string
		}{
			{snapshots.SegmentHistory, ".seg"},
			{snapshots.SegmentInverted, ".idx"},
			{snapshots.SegmentAccessor, ".kv"},
		}
		for _, spec := range specs {
			rel := base + spec.ext
			full := filepath.Join(dir, rel)
			if err := os.WriteFile(full, []byte{1}, 0o600); err != nil {
				t.Fatal(err)
			}
			refs = append(refs, snapshots.SegmentRef{
				Dataset:   snapshots.SegmentDatasetStateDomainChange,
				Kind:      spec.kind,
				FromTxNum: from,
				ToTxNum:   to,
				Path:      rel,
				Size:      1,
				Checksum:  "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			})
		}
	}
	manifest := snapshots.NewManifest(0, uint64(count)*10-1, refs)
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}

	w := Worker{SnapshotDir: dir, Policy: SnapPolicy(1, 1)}
	gate, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantGate, err := scanSnapshotCoverageGate(w, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gate.segments, wantGate.segments) {
		t.Fatal("parallel stat gate segments mismatch sequential scan")
	}

	// Remove files for segments 51 and 17; manifest-order error must pick index 17 first.
	for _, idx := range []int{51, 17} {
		from := uint64(idx) * 10
		to := from + 9
		_ = os.Remove(filepath.Join(dir, fmt.Sprintf("history/state-domain-change-%d-%d.seg", from, to)))
	}
	_, gotErr := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
	_, wantErr := scanSnapshotCoverageGate(w, context.Background())
	if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
		t.Fatalf("parallel error order mismatch: got=%v want=%v", gotErr, wantErr)
	}
}

func TestSnapshotCoverageVerificationCachePersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cache := newSnapshotCoverageVerificationCache(dir)
	for i := 0; i < 8; i++ {
		from := uint64(i) * 100
		to := from + 99
		rec := snapshotHistoryVerificationRecord{
			History: snapshots.SegmentRef{
				Dataset:   snapshots.SegmentDatasetStateDomainChange,
				Kind:      snapshots.SegmentHistory,
				FromTxNum: from,
				ToTxNum:   to,
				Path:      fmt.Sprintf("history/seg-%d.seg", i),
				Size:      4096,
				Checksum:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			Index: snapshots.SegmentRef{
				Dataset:   snapshots.SegmentDatasetStateDomainChange,
				Kind:      snapshots.SegmentInverted,
				FromTxNum: from,
				ToTxNum:   to,
				Path:      fmt.Sprintf("history/seg-%d.idx", i),
				Size:      1024,
				Checksum:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			Accessor: snapshots.SegmentRef{
				Dataset:   snapshots.SegmentDatasetStateDomainChange,
				Kind:      snapshots.SegmentAccessor,
				FromTxNum: from,
				ToTxNum:   to,
				Path:      fmt.Sprintf("history/seg-%d.kvi", i),
				Size:      512,
				Checksum:  "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			},
		}
		cache.persistent[rec] = struct{}{}
	}
	cache.mu.Lock()
	err := cache.persistLocked()
	cache.mu.Unlock()
	if err != nil {
		t.Fatalf("persistLocked: %v", err)
	}

	reloaded := newSnapshotCoverageVerificationCache(dir)
	if err := reloaded.LoadError(); err != nil {
		t.Fatalf("reload error: %v", err)
	}
	if !reflect.DeepEqual(reloaded.persistent, cache.persistent) {
		t.Fatalf("reloaded cache mismatch: got %d entries, want %d", len(reloaded.persistent), len(cache.persistent))
	}
}

func TestWorkerPruneToContextLazyStatGateSkipsUnqueriedAndAuthenticatesQueried(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	writeSnapPruningChange(t, db, 2, 20, 22)

	firstRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	secondRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 20, 22, "history/state-domain-change-20-22.seg")
	if err != nil {
		t.Fatal(err)
	}
	allRefs := append(append([]snapshots.SegmentRef(nil), firstRefs...), secondRefs...)
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 22, allRefs)); err != nil {
		t.Fatal(err)
	}

	// Pre-populate a stale retired record in the verification cache so retainRecords
	// is exercised alongside lazy gate construction.
	cache := newSnapshotCoverageVerificationCache(dir)
	staleRec := snapshotHistoryVerificationRecord{
		History: snapshots.SegmentRef{
			Dataset:   snapshots.SegmentDatasetStateDomainChange,
			Kind:      snapshots.SegmentHistory,
			FromTxNum: 1,
			ToTxNum:   2,
			Path:      "history/retired-stale.seg",
			Size:      4096,
			Checksum:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		Index: snapshots.SegmentRef{
			Dataset:   snapshots.SegmentDatasetStateDomainChange,
			Kind:      snapshots.SegmentInverted,
			FromTxNum: 1,
			ToTxNum:   2,
			Path:      "history/retired-stale.idx",
			Size:      1024,
			Checksum:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		Accessor: snapshots.SegmentRef{
			Dataset:   snapshots.SegmentDatasetStateDomainChange,
			Kind:      snapshots.SegmentAccessor,
			FromTxNum: 1,
			ToTxNum:   2,
			Path:      "history/retired-stale.kvi",
			Size:      512,
			Checksum:  "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		},
	}
	staleKey := snapshotHistoryVerificationKey{history: staleRec.History, index: staleRec.Index, accessor: staleRec.Accessor}
	cache.mu.Lock()
	cache.verified[staleKey] = struct{}{}
	cache.persistent[staleRec] = struct{}{}
	cache.mu.Unlock()

	// Remove the unqueried second history file on disk before pruning block 1.
	secondHistory := historyRef(t, secondRefs)
	if err := os.Remove(filepath.Join(dir, secondHistory.Path)); err != nil {
		t.Fatal(err)
	}

	worker := Worker{
		DB:                        db,
		Policy:                    SnapPolicy(1, 1),
		SnapshotDir:               dir,
		MaxBlocks:                 1,
		coverageVerificationCache: cache,
	}
	first, err := worker.PruneToContext(context.Background(), 5)
	if err != nil {
		t.Fatalf("first prune over healthy segment 1 failed: %v", err)
	}
	if first.DeletedDomainChangeBlocks != 1 || first.DomainChangePrunedThrough != 1 {
		t.Fatalf("first prune stats = %+v, want block 1 deleted", first)
	}
	stats := cache.Stats()
	if stats.ActiveEntries != 2 || stats.ActiveBytes == 0 || stats.Entries != 1 || stats.FullVerified != 1 {
		t.Fatalf("cache stats after lazy gate = %+v, want ActiveEntries=2 Entries=1 FullVerified=1", stats)
	}
	cache.mu.Lock()
	_, staleInMem := cache.verified[staleKey]
	_, staleOnDisk := cache.persistent[staleRec]
	cache.mu.Unlock()
	if staleInMem || staleOnDisk {
		t.Fatalf("retainRecords did not evict retired record: inMem=%t onDisk=%t", staleInMem, staleOnDisk)
	}

	// Pruning block 2 queries segment 2 and must fail fresh os.Stat authentication.
	if _, err := worker.PruneToContext(context.Background(), 5); err == nil {
		t.Fatal("expected prune touching removed segment 2 to fail fresh verification")
	}
}

func BenchmarkHistoryCompanionGateCovers(b *testing.B) {
	const count = 5000
	segments := make([]snapshotStateDomainCoverageSegment, count)
	prefixMaxTo := make([]uint64, count)
	for i := 0; i < count; i++ {
		from := uint64(i) * 1000
		to := from + 999
		segments[i] = snapshotStateDomainCoverageSegment{
			ref:      snapshots.SegmentRef{FromTxNum: from, ToTxNum: to},
			verified: true,
		}
		prefixMaxTo[i] = to
	}
	gate := &snapshotStateDomainCoverageGate{
		segments:    segments,
		prefixMaxTo: prefixMaxTo,
	}
	targetFrom := uint64(count-2) * 1000
	targetTo := targetFrom + 500
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := gate.covers(targetFrom, targetTo)
		if err != nil || !ok {
			b.Fatalf("covers: %v %v", ok, err)
		}
	}
}
