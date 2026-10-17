package snapshots

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func costCacheLeaf(t testing.TB, dir string, from, count uint64) []SegmentRef {
	t.Helper()
	return writeCompressedV6HistorySpaceTrio(t, dir, from, from+count-1, v6StreamChanges(from, count, 1, 96))
}

func costCacheCandidates(t testing.TB, refs []SegmentRef) []historyCompactionCandidate {
	t.Helper()
	domain, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	return historyCompactionCandidates(NewManifest(1, refs[len(refs)-1].ToTxNum, refs), domain)
}

// This exercises the actual compressed-history footer and V6 accessor reader,
// not synthetic map lookups. Old leaves exceed the record limit while remaining
// far below the physical limit, matching the production rejection pattern.
func TestHistoryCostCacheAcrossRealCompressedPasses(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const oldLeaves, oldRecords, passes = 256, 8, 5
	var refs []SegmentRef
	for i := uint64(0); i < oldLeaves; i++ {
		refs = append(refs, costCacheLeaf(t, dir, i*oldRecords+1, oldRecords)...)
	}
	for i := uint64(0); i < 4; i++ {
		refs = append(refs, costCacheLeaf(t, dir, oldLeaves*oldRecords+i+1, 1)...)
	}
	if err := PublishManifest(dir, NewManifest(1, oldLeaves*oldRecords+4, refs)); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	domain, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	candidates := historyCompactionCandidates(manifest, domain)
	cfg := CompactionConfig{BusyLeafOnly: true, MaxInputRecords: 4, MaxInputBytes: busyHistoryCompactionInputBytes, MaxInputLogicalBytes: busyHistoryCompactionLogicalBytes, MaxSources: 4}
	baselineReads, cachedReads := 0, 0
	baselineReader := func(c historyCompactionCandidate) (historyCompactionInputCost, error) {
		baselineReads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	reader := func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		cachedReads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	cache := newHistoryCompactionCostCache()
	cacheStats := 0
	cache.stat = func(path string) (os.FileInfo, error) { cacheStats++; return os.Stat(path) }
	baselineStart := time.Now()
	var want historyCompactionSelection
	for pass := 0; pass < passes; pass++ {
		var ok bool
		want, ok, err = selectBudgetedHistoryCompactionLeaves(ctx, candidates, cfg, baselineReader)
		if err != nil || !ok || want.fromTxNum != oldLeaves*oldRecords+1 || want.inputRecords != 4 {
			t.Fatalf("uncached selection=%+v ok=%v err=%v", want, ok, err)
		}
	}
	baselineElapsed := time.Since(baselineStart)
	cachedStart := time.Now()
	for pass := 0; pass < passes; pass++ {
		if err := cache.bind(ctx, dir, 0); err != nil {
			t.Fatal(err)
		}
		got, ok, err := selectBudgetedHistoryCompactionLeaves(ctx, candidates, cfg, func(c historyCompactionCandidate) (historyCompactionInputCost, error) {
			return cache.read(ctx, c, reader)
		})
		if err != nil || !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("cached selection mismatch pass%d: %+v %v %v", pass, got, ok, err)
		}
		// Count the mandatory selected-input fresh reads in the cached total.
		for _, c := range got.candidates {
			if _, err := cache.readFresh(ctx, c, reader); err != nil {
				t.Fatal(err)
			}
		}
	}
	if baselineReads != passes*(oldLeaves+4) || cachedReads != oldLeaves+4+passes*4 {
		t.Fatalf("real metadata probes uncached=%d cached=%d", baselineReads, cachedReads)
	}
	// The original reader makes 3 path Stats and 2 opened-file Stats for these
	// raw-accessor/compressed-history trios. A cache hit still makes 3 Stats;
	// a miss adds before/after 3-file observations, and selected fresh reads add 6.
	baselineStats := baselineReads * 5
	wantCacheStats := passes*(oldLeaves+4)*3 + (oldLeaves+4)*3 + passes*4*6 + passes
	if cacheStats != wantCacheStats {
		t.Fatalf("actual cache Stats=%d, want path model=%d", cacheStats, wantCacheStats)
	}
	cachedStats := cachedReads*5 + cacheStats
	t.Logf("same %d-leaf manifest, %d passes: actual full metadata-reader calls %d -> %d; measured additional cache Stats=%d; total Stat model (5/original reader + measured cache) %d -> %d; wall sequential warm-FS %s -> %s; retained charge=%d", len(candidates), passes, baselineReads, cachedReads, cacheStats, baselineStats, cachedStats, baselineElapsed, time.Since(cachedStart), cache.bytes)
}

func TestHistoryCostCacheIdentityAndErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	refs := costCacheLeaf(t, dir, 1, 3)
	candidate := costCacheCandidates(t, refs)[0]
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	reads := 0
	reader := func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		reads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	load := func() {
		t.Helper()
		if _, err := cache.read(ctx, candidate, reader); err != nil {
			t.Fatal(err)
		}
	}
	load()
	load()
	if reads != 1 {
		t.Fatalf("stable input reopened %d times", reads)
	}
	// Replacing a same-size file must miss even when its timestamp is restored.
	accessor, ok := historyCompactionCompanion(candidate, SegmentAccessor)
	if !ok {
		t.Fatal("missing accessor")
	}
	path := filepath.Join(dir, accessor.Path)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".replacement", original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path+".replacement", info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".replacement", path); err != nil {
		t.Fatal(err)
	}
	load()
	if reads != 2 {
		t.Fatal("same-size replacement was reused")
	}
	// A same-inode/size content change with a new mtime cannot return old cost.
	bad := append([]byte(nil), original...)
	bad[0] ^= 0xff
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	later := info.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(ctx, candidate, reader); err == nil {
		t.Fatal("corrupt accessor accepted")
	}
	if len(cache.entries) != 0 {
		t.Fatal("error retained old cache entry")
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	load()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	before := reads
	if _, err := cache.read(ctx, candidate, reader); !os.IsNotExist(err) {
		t.Fatalf("missing file error=%v", err)
	}
	if reads != before {
		t.Fatal("missing-file Stat still read headers")
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	load()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	before = reads
	if _, err := cache.read(canceled, candidate, reader); !errors.Is(err, context.Canceled) || reads != before {
		t.Fatal("canceled lookup read metadata", err)
	}
	cache.clear()
	if _, err := cache.readFresh(ctx, candidate, func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		cost, err := reader(ctx, dir, c)
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		return cost, errors.Join(err, cancelCtx.Err())
	}); !errors.Is(err, context.Canceled) || len(cache.entries) != 0 {
		t.Fatal("canceled reader result cached", err)
	}
	load()
	if err := cache.bind(ctx, dir, 1); err != nil || len(cache.entries) != 0 {
		t.Fatal("epoch did not invalidate", err)
	}
	load()
	old := dir + "-old"
	if err := os.Rename(dir, old); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(old)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := cache.bind(ctx, dir, 1); err != nil || len(cache.entries) != 0 {
		t.Fatal("directory replacement did not invalidate", err)
	}
}

func TestHistoryCostCacheFullKeyOwnershipAndCapacity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	refs := costCacheLeaf(t, dir, 1, 2)
	candidate := costCacheCandidates(t, refs)[0]
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	reads := 0
	reader := func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		reads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	if _, err := cache.read(ctx, candidate, reader); err != nil {
		t.Fatal(err)
	}
	changed := candidate
	changed.companions = append([]SegmentRef(nil), candidate.companions...)
	changed.companions[0].Checksum = strings.Repeat("1", 64)
	if _, err := cache.read(ctx, changed, reader); err != nil {
		t.Fatal(err)
	}
	if reads != 2 {
		t.Fatal("companion identity omitted from key")
	}
	changed.companions[0].Path = "missing"
	if _, err := cache.read(ctx, changed, reader); !os.IsNotExist(err) {
		t.Fatal("companion path change swallowed", err)
	}
	// Borrow small substrings of a large slab for every string field. Cache
	// ownership must not pin that slab through either map keys or entry values.
	key, ok := historyCompactionCacheKey(candidate)
	if !ok {
		t.Fatal("fixture not cacheable")
	}
	var borrowed []string
	for i := range key {
		cloneBacking := func(s string) string {
			big := strings.Repeat("x", 1<<20) + s + strings.Repeat("y", 1<<20)
			v := big[1<<20 : (1<<20)+len(s)]
			borrowed = append(borrowed, v)
			return v
		}
		key[i].Dataset = SegmentDataset(cloneBacking(string(key[i].Dataset)))
		key[i].Kind = SegmentKind(cloneBacking(string(key[i].Kind)))
		key[i].Path = cloneBacking(key[i].Path)
		key[i].Checksum = cloneBacking(key[i].Checksum)
	}
	files, err := statHistoryCompactionInputs(ctx, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	cache.clear()
	cache.remember(key, files, historyCompactionInputCost{records: 2})
	for stored := range cache.entries {
		n := 0
		for _, ref := range stored {
			for _, s := range []string{string(ref.Dataset), string(ref.Kind), ref.Path, ref.Checksum} {
				if unsafe.StringData(s) == unsafe.StringData(borrowed[n]) {
					t.Fatal("cache key retained large borrowed backing")
				}
				n++
			}
		}
	}
	cache.clear()
	cache.maxEntries = 1
	cache.remember(key, files, historyCompactionInputCost{records: 2})
	other := key
	other[0].Path += "-new"
	cache.remember(other, files, historyCompactionInputCost{})
	if len(cache.entries) != 1 {
		t.Fatal("entry cap exceeded")
	}
	if _, ok := cache.entries[key]; !ok {
		t.Fatal("full scan evicted stable prefix")
	}
	if cache.bytes > cache.maxBytes {
		t.Fatal("byte cap exceeded")
	}
	cache.clear()
	cache.maxBytes = 1
	cache.remember(key, files, historyCompactionInputCost{})
	if len(cache.entries) != 0 || cache.bytes != 0 {
		t.Fatal("oversize metadata retained")
	}
	unknown := candidate
	unknown.history.Size = 0
	cache.maxBytes = historyCostCacheBytes
	if _, err := cache.read(ctx, unknown, reader); err != nil {
		t.Fatal(err)
	}
	if len(cache.entries) != 0 {
		t.Fatal("unknown size cached")
	}
}

func TestHistoryCostCacheFreshSelectionRejectsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	refs := costCacheLeaf(t, dir, 1, 3)
	refs = append(refs, costCacheLeaf(t, dir, 4, 1)...)
	refs = append(refs, costCacheLeaf(t, dir, 5, 1)...)
	if err := PublishManifest(dir, NewManifest(1, 5, refs)); err != nil {
		t.Fatal(err)
	}
	cache := newHistoryCompactionCostCache()
	ctx := context.Background()
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	candidates := costCacheCandidates(t, refs)
	first := candidates[0]
	if _, err := cache.read(ctx, first, readHistoryCompactionInputCost); err != nil {
		t.Fatal(err)
	}
	key, _ := historyCompactionCacheKey(first)
	stale := cache.entries[key]
	stale.cost.records = 1
	cache.entries[key] = stale // simulate an obsolete admission observation
	cfg := CompactionConfig{BusyLeafOnly: true, MaxInputBytes: busyHistoryCompactionInputBytes, MaxInputLogicalBytes: busyHistoryCompactionLogicalBytes, MaxInputRecords: 2, MaxSources: 4, inputCostCache: cache}
	before, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	beforeFiles, err := os.ReadDir(filepath.Join(dir, "history"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, cfg)
	if err != nil || result.Merged || result.DeferReason != "input-cost-changed" {
		t.Fatalf("stale selection not rejected: %+v %v", result, err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	afterFiles, _ := os.ReadDir(filepath.Join(dir, "history"))
	if !bytes.Equal(before, after) || !reflect.DeepEqual(beforeFiles, afterFiles) {
		t.Fatal("rejected selection wrote outputs or manifest")
	}
	result, err = CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, cfg)
	if err != nil || !result.Merged || result.FromTxNum != 4 || result.ToTxNum != 5 {
		t.Fatalf("updated costs did not recover next pass: %+v %v", result, err)
	}
}

func TestHistoryCostCacheDoesNotReplacePayloadAuthentication(t *testing.T) {
	dir := t.TempDir()
	refs := append(costCacheLeaf(t, dir, 1, 1), costCacheLeaf(t, dir, 2, 1)...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(context.Background(), dir, 0); err != nil {
		t.Fatal(err)
	}
	for _, c := range costCacheCandidates(t, refs) {
		if _, err := cache.read(context.Background(), c, readHistoryCompactionInputCost); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, refs[0].Path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 1
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, info.Mode()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxInputRecords: 2, MaxSources: 2, inputCostCache: cache})
	if err == nil || result.Merged {
		t.Fatal("cache bypassed payload authentication", result, err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	if !bytes.Equal(before, after) {
		t.Fatal("corrupt input changed manifest")
	}
}

func TestHistoryCostCacheLongPathsAndCapacityDegradation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var refs []SegmentRef
	for i := uint64(0); i < 5; i++ {
		refs = append(refs, costCacheLeaf(t, dir, i*3+1, 3)...)
	}
	candidates := costCacheCandidates(t, refs)
	cache := newHistoryCompactionCostCache()
	cache.maxEntries = 2
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	reads := 0
	reader := func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		reads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	for pass := 0; pass < 4; pass++ {
		for _, candidate := range candidates {
			if _, err := cache.read(ctx, candidate, reader); err != nil {
				t.Fatal(err)
			}
		}
	}
	// At capacity preserve the first two stable entries. The suffix still does
	// real metadata IO each pass; no O(N+selected) claim applies past the cap.
	if reads != 5+3*3 || len(cache.entries) != 2 {
		t.Fatalf("cap degradation: reads=%d entries=%d", reads, len(cache.entries))
	}
	key, _ := historyCompactionCacheKey(candidates[0])
	files, err := statHistoryCompactionInputs(ctx, dir, key)
	if err != nil {
		t.Fatal(err)
	}
	long := key
	for i := range long {
		long[i].Path = strings.Repeat("d/", 1400) + "leaf"
	}
	charge, ok := historyCompactionCostCacheCharge(dir, long)
	if !ok || charge < 2*3*2*uint64(len(long[0].Path))+2048 {
		t.Fatal("long Stat path or cloned ref backing omitted", charge)
	}
	cache.clear()
	cache.maxEntries = historyCostCacheEntries
	cache.maxBytes = charge - 1
	cache.remember(long, files, historyCompactionInputCost{})
	if len(cache.entries) != 0 || cache.bytes != 0 {
		t.Fatal("long key exceeded metadata cap")
	}
	cache.maxBytes = charge
	cache.remember(long, files, historyCompactionInputCost{})
	if len(cache.entries) != 1 || cache.bytes != charge {
		t.Fatal("exact metadata boundary rejected", cache.bytes, charge)
	}
}

func TestHistoryCostCacheProductionBudgetBoundary(t *testing.T) {
	cfg := CompactionConfig{MaxInputBytes: busyHistoryCompactionInputBytes, MaxInputLogicalBytes: busyHistoryCompactionLogicalBytes, MaxInputRecords: busyHistoryCompactionInputRecords, MaxSources: busyHistoryCompactionSources}
	if !historyCompactionWithinBudget(cfg, 128<<20, 512<<20, 400_000, 4) || historyCompactionWithinBudget(cfg, 128<<20, 512<<20, 400_001, 4) || historyCompactionWithinBudget(cfg, 128<<20, 512<<20, 400_000, 5) {
		t.Fatal("production admission boundary changed")
	}
}

func TestHistoryCostCacheRejectsMutationDuringObservation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	candidate := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 2))[0]
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	accessor, _ := historyCompactionCompanion(candidate, SegmentAccessor)
	path := filepath.Join(dir, accessor.Path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cache.read(ctx, candidate, func(ctx context.Context, dir string, candidate historyCompactionCandidate) (historyCompactionInputCost, error) {
		cost, err := readHistoryCompactionInputCost(ctx, dir, candidate)
		if err != nil {
			return cost, err
		}
		return cost, os.Chtimes(path, info.ModTime(), info.ModTime().Add(time.Second))
	})
	if err == nil || len(cache.entries) != 0 {
		t.Fatalf("changed observation cached: err=%v entries=%d", err, len(cache.entries))
	}
	if _, err := cache.read(ctx, candidate, readHistoryCompactionInputCost); err != nil {
		t.Fatal(err)
	}
	if len(cache.entries) != 1 {
		t.Fatal("stable retry was not cached")
	}
}
