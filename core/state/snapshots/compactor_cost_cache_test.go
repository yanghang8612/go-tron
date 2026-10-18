package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
	// Ordinary hint hits/misses add no file observations. Mandatory selected
	// reads still add before/after observations and use the full original reader.
	wantStats := passes*4*6 + passes
	if cacheStats != wantStats {
		t.Fatalf("actual cache Stats=%d want=%d", cacheStats, wantStats)
	}
	if s := cache.snapshot(); s.Hits != 1040 || s.Misses != 260 || s.MetadataReads != 280 || s.FreshReads != 20 || s.Entries != 260 || s.ChargedBytes != 260*512 {
		t.Fatal("hint statistics", s)
	}
	t.Logf("metadata readers %d -> %d; cache Stat calls %d; sequential warm-FS elapsed %s -> %s (not device IOPS)", baselineReads, cachedReads, cacheStats, baselineElapsed, time.Since(cachedStart))
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
	stale.records = 1
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

func TestHistoryCostCacheProductionBudgetBoundary(t *testing.T) {
	cfg := CompactionConfig{MaxInputBytes: busyHistoryCompactionInputBytes, MaxInputLogicalBytes: busyHistoryCompactionLogicalBytes, MaxInputRecords: busyHistoryCompactionInputRecords, MaxSources: busyHistoryCompactionSources}
	if !historyCompactionWithinBudget(cfg, 128<<20, 512<<20, 400_000, 4) || historyCompactionWithinBudget(cfg, 128<<20, 512<<20, 400_001, 4) || historyCompactionWithinBudget(cfg, 128<<20, 512<<20, 400_000, 5) {
		t.Fatal("production admission boundary changed")
	}
}

func TestHistoryCostCacheRejectsMutationDuringObservation(t *testing.T) {
	for _, mutation := range []string{"mtime", "replacement"} {
		t.Run(mutation, func(t *testing.T) {
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
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = cache.readFresh(ctx, candidate, func(ctx context.Context, dir string, candidate historyCompactionCandidate) (historyCompactionInputCost, error) {
				cost, err := readHistoryCompactionInputCost(ctx, dir, candidate)
				if err != nil {
					return cost, err
				}
				if mutation == "mtime" {
					return cost, os.Chtimes(path, info.ModTime(), info.ModTime().Add(time.Second))
				}
				replacement := path + ".replacement"
				if err := os.WriteFile(replacement, original, info.Mode()); err != nil {
					return cost, err
				}
				if err := os.Chtimes(replacement, info.ModTime(), info.ModTime()); err != nil {
					return cost, err
				}
				return cost, os.Rename(replacement, path)
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
		})
	}
}

func TestHistoryCostHintsRawFieldEncodingAndOwnership(t *testing.T) {
	// An explicit schema inventory fails if production adds an unencoded field.
	expected := []string{"Rows", "Dataset", "Domain", "Kind", "FromTxNum", "ToTxNum", "AggregationSteps", "Path", "Size", "Checksum"}
	typ := reflect.TypeOf(SegmentRef{})
	if typ.NumField() != len(expected) {
		t.Fatal("SegmentRef schema changed; update canonical hint encoding")
	}
	for i, n := range expected {
		if typ.Field(i).Name != n {
			t.Fatal("unexpected ref field", typ.Field(i))
		}
	}
	dir := t.TempDir()
	candidate := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 2))[0]
	refs, ok := historyCompactionCacheRefs(candidate)
	if !ok {
		t.Fatal("not cacheable")
	}
	original := digestHistoryCompactionRefs(refs)
	for slot := range refs {
		for i, n := range expected {
			changed := refs
			v := reflect.ValueOf(&changed[slot]).Elem().Field(i)
			switch v.Kind() {
			case reflect.String:
				v.SetString(v.String() + "x")
			case reflect.Uint16, reflect.Uint64:
				v.SetUint(v.Uint() + 1)
			default:
				t.Fatalf("unhandled field %s type %s", n, v.Type())
			}
			if digestHistoryCompactionRefs(changed) == original {
				t.Fatalf("slot %d field %s omitted", slot, n)
			}
		}
	}
	reversed := candidate
	reversed.companions = []SegmentRef{candidate.companions[1], candidate.companions[0]}
	if key, ok := historyCompactionCacheKey(reversed); !ok || key != original {
		t.Fatal("companion canonical order")
	}
	reversed.companions[1] = reversed.companions[0]
	if _, ok := historyCompactionCacheKey(reversed); ok {
		t.Fatal("duplicate companion accepted")
	}
	a, b := refs, refs
	a[0].Dataset = "a"
	a[0].Kind = "bc"
	b[0].Dataset = "ab"
	b[0].Kind = "c"
	if digestHistoryCompactionRefs(a) == digestHistoryCompactionRefs(b) {
		t.Fatal("ambiguous field encoding")
	}
	if unsafe.Sizeof(historyCompactionCostKey{}) != sha256.Size || unsafe.Sizeof(historyCompactionInputCost{}) != 24 || historyCostCacheEntries != 131072 || historyCostCacheEntryCharge != 512 || historyCostCacheBytes != 64<<20 {
		t.Fatal("hint size/charge/budget changed")
	}
	for i := 0; i < reflect.TypeOf(historyCompactionInputCost{}).NumField(); i++ {
		if reflect.TypeOf(historyCompactionInputCost{}).Field(i).Type.Kind() != reflect.Uint64 {
			t.Fatal("hint value retains managed backing")
		}
	}
	t.Logf("digest=%d cost=%d per-entry charge=%d", unsafe.Sizeof(historyCompactionCostKey{}), unsafe.Sizeof(historyCompactionInputCost{}), historyCostCacheEntryCharge)
}

func TestHistoryCostHintsDeferUnselectedIdentityErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	candidate := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 3))[0]
	cache := newHistoryCompactionCostCache()
	stats := 0
	cache.stat = func(path string) (os.FileInfo, error) { stats++; return os.Stat(path) }
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	reads := 0
	reader := func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		reads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	want, err := cache.read(ctx, candidate, reader)
	if err != nil {
		t.Fatal(err)
	}
	accessor, _ := historyCompactionCompanion(candidate, SegmentAccessor)
	path := filepath.Join(dir, accessor.Path)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	got, err := cache.read(ctx, candidate, reader)
	if err != nil || got != want || reads != 1 || stats != 1 {
		t.Fatal("unselected hint must not Stat/reopen", got, err, reads, stats)
	}
	if _, err := cache.readFresh(ctx, candidate, reader); !os.IsNotExist(err) || len(cache.entries) != 0 {
		t.Fatal("selected missing file not rejected/evicted", err)
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(ctx, candidate, reader); err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(original)
	bad[0] ^= 0xff
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(ctx, candidate, reader); err != nil {
		t.Fatal("unselected hint unexpectedly authenticates", err)
	}
	if _, err := cache.readFresh(ctx, candidate, reader); err == nil || len(cache.entries) != 0 {
		t.Fatal("selected corrupted header accepted")
	}
	if err := cache.bind(ctx, dir, 1); err != nil || len(cache.entries) != 0 {
		t.Fatal("reset failed", err)
	}
	if _, err := cache.read(ctx, candidate, reader); err == nil {
		t.Fatal("reset repaired corrupt bytes")
	}
}

func TestHistoryCostHintsResetCancellationAndFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 2))[0]
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(ctx, c, readHistoryCompactionInputCost); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := cache.read(canceled, c, readHistoryCompactionInputCost); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cache.clear()
	cancelDuring, stop := context.WithCancel(ctx)
	if _, err := cache.read(cancelDuring, c, func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		cost, err := readHistoryCompactionInputCost(ctx, dir, c)
		stop()
		return cost, err
	}); !errors.Is(err, context.Canceled) || len(cache.entries) != 0 {
		t.Fatal("canceled miss remembered", err)
	}
	failure := errors.New("reader failure")
	if _, err := cache.read(ctx, c, func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error) {
		return historyCompactionInputCost{}, failure
	}); !errors.Is(err, failure) || len(cache.entries) != 0 {
		t.Fatal("failed miss cached", err)
	}
	if _, err := cache.read(ctx, c, readHistoryCompactionInputCost); err != nil {
		t.Fatal(err)
	}
	if err := cache.bind(ctx, dir, 1); err != nil || len(cache.entries) != 0 {
		t.Fatal("epoch did not clear", err)
	}
	if _, err := cache.read(ctx, c, readHistoryCompactionInputCost); err != nil {
		t.Fatal(err)
	}
	old := dir + "-old"
	if err := os.Rename(dir, old); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(old)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := cache.bind(ctx, dir, 1); err != nil || len(cache.entries) != 0 {
		t.Fatal("directory replacement did not clear", err)
	}
	uncacheable := c
	uncacheable.companions = nil
	if _, err := cache.read(ctx, uncacheable, func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error) {
		return historyCompactionInputCost{}, failure
	}); !errors.Is(err, failure) {
		t.Fatal("noncacheable reader error suppressed", err)
	}
}

func TestHistoryCostHintsCapacityAndRetirement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var refs []SegmentRef
	for i := uint64(0); i < 5; i++ {
		refs = append(refs, costCacheLeaf(t, dir, i*3+1, 3)...)
	}
	candidates := costCacheCandidates(t, refs)
	cache := newHistoryCompactionCostCache()
	cache.maxBytes = 2 * historyCostCacheEntryCharge
	if err := cache.bind(ctx, dir, 0); err != nil {
		t.Fatal(err)
	}
	reads := 0
	reader := func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		reads++
		return readHistoryCompactionInputCost(ctx, dir, c)
	}
	for pass := 0; pass < 4; pass++ {
		for _, c := range candidates {
			if _, err := cache.read(ctx, c, reader); err != nil {
				t.Fatal(err)
			}
		}
	}
	if reads != 14 || len(cache.entries) != 2 || cache.bytes != 1024 || cache.snapshot().CapacityRejects != 12 {
		t.Fatal("bounded stable prefix", reads, cache.snapshot())
	}
	cache.forgetRetired(candidates[:1])
	if len(cache.entries) != 1 || cache.bytes != 512 || cache.snapshot().EvictedRetired != 1 {
		t.Fatal("retired charge not released", cache.snapshot())
	}
	cache.forgetRetired(candidates[:1])
	if cache.snapshot().EvictedRetired != 1 {
		t.Fatal("absent removal counted")
	}
	if _, err := cache.read(ctx, candidates[2], reader); err != nil || len(cache.entries) != 2 {
		t.Fatal("released capacity unavailable", err)
	}
	for _, maxBytes := range []uint64{511, 512} {
		cache.clear()
		cache.maxBytes = maxBytes
		key, _ := historyCompactionCacheKey(candidates[0])
		cache.remember(key, historyCompactionInputCost{})
		if (len(cache.entries) == 1) != (maxBytes == 512) {
			t.Fatal("exact budget boundary", cache.snapshot())
		}
	}
}

func TestHistoryCostHintOverestimateMaySkipUnselectedDamage(t *testing.T) {
	dir := t.TempDir()
	refs := costCacheLeaf(t, dir, 1, 3)
	refs = append(refs, costCacheLeaf(t, dir, 4, 1)...)
	refs = append(refs, costCacheLeaf(t, dir, 5, 1)...)
	if err := PublishManifest(dir, NewManifest(1, 5, refs)); err != nil {
		t.Fatal(err)
	}
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(context.Background(), dir, 0); err != nil {
		t.Fatal(err)
	}
	candidates := costCacheCandidates(t, refs)
	key, _ := historyCompactionCacheKey(candidates[0])
	cache.remember(key, historyCompactionInputCost{records: ^uint64(0)})
	if err := os.Remove(filepath.Join(dir, candidates[0].history.Path)); err != nil {
		t.Fatal(err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxInputRecords: 2, MaxSources: 2, inputCostCache: cache})
	if err != nil || !result.Merged || result.FromTxNum != 4 || result.ToTxNum != 5 {
		t.Fatal("healthy later group blocked by unselected hint", result, err)
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	kept := false
	for _, ref := range manifest.Segments {
		kept = kept || ref == candidates[0].history
	}
	if !kept {
		t.Fatal("unselected source retired")
	}
	if cache.snapshot().EvictedRetired != 2 {
		t.Fatal("published selected hints not removed", cache.snapshot())
	}
	// Reset clears the hint, not the damage: the original missing-file error returns.
	if err := cache.bind(context.Background(), dir, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(context.Background(), candidates[0], readHistoryCompactionInputCost); !os.IsNotExist(err) {
		t.Fatal("missing source repaired by reset", err)
	}
}

// A hint cannot authorize a selected group, regardless of which work dimension
// was stale or whether it under/overestimated the selected input.
func TestHistoryCostHintsSelectedCostDimensions(t *testing.T) {
	for _, dimension := range []string{"bytes", "logical", "records"} {
		for _, direction := range []string{"low", "high"} {
			t.Run(dimension+"/"+direction, func(t *testing.T) {
				dir := t.TempDir()
				refs := append(costCacheLeaf(t, dir, 1, 1), costCacheLeaf(t, dir, 2, 1)...)
				if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
					t.Fatal(err)
				}
				cache := newHistoryCompactionCostCache()
				if err := cache.bind(context.Background(), dir, 0); err != nil {
					t.Fatal(err)
				}
				candidates := costCacheCandidates(t, refs)
				for _, c := range candidates {
					if _, err := cache.read(context.Background(), c, readHistoryCompactionInputCost); err != nil {
						t.Fatal(err)
					}
				}
				key, _ := historyCompactionCacheKey(candidates[0])
				cost := cache.entries[key]
				field := &cost.bytes
				if dimension == "logical" {
					field = &cost.logicalBytes
				} else if dimension == "records" {
					field = &cost.records
				}
				if *field == 0 {
					t.Fatal("fixture has no work in tested dimension")
				}
				if direction == "low" {
					*field--
				} else {
					*field++
				}
				cache.entries[key] = cost
				before, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
				beforeFiles, _ := os.ReadDir(filepath.Join(dir, "history"))
				result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxSources: 2, inputCostCache: cache})
				if err != nil || result.Merged || result.DeferReason != "input-cost-changed" {
					t.Fatal("changed hint authorized selected group", result, err)
				}
				after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
				afterFiles, _ := os.ReadDir(filepath.Join(dir, "history"))
				if !bytes.Equal(before, after) || !reflect.DeepEqual(beforeFiles, afterFiles) {
					t.Fatal("changed hint caused publication/output")
				}
				result, err = CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: true, MaxSources: 2, inputCostCache: cache})
				if err != nil || !result.Merged {
					t.Fatal("fresh replacement did not recover", result, err)
				}
			})
		}
	}
}

func TestHistoryCostHintsFreshCancellationAndSelectedMissing(t *testing.T) {
	for _, kind := range []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor} {
		t.Run(string(kind), func(t *testing.T) {
			dir := t.TempDir()
			c := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 1))[0]
			cache := newHistoryCompactionCostCache()
			if err := cache.bind(context.Background(), dir, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := cache.read(context.Background(), c, readHistoryCompactionInputCost); err != nil {
				t.Fatal(err)
			}
			refs, _ := historyCompactionCacheRefs(c)
			for _, ref := range refs {
				if ref.Kind == kind {
					if err := os.Remove(filepath.Join(dir, ref.Path)); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := cache.snapshot()
			if _, err := cache.readFresh(context.Background(), c, readHistoryCompactionInputCost); !os.IsNotExist(err) || len(cache.entries) != 0 {
				t.Fatal("selected missing accepted", err)
			}
			if got := cache.snapshot(); got.FreshReads != before.FreshReads || got.MetadataReads != before.MetadataReads {
				t.Fatal("before-Stat failure counted a reader call", got)
			}
		})
	}
	dir := t.TempDir()
	c := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 1))[0]
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(context.Background(), dir, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.read(context.Background(), c, readHistoryCompactionInputCost); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := cache.readFresh(ctx, c, func(ctx context.Context, dir string, c historyCompactionCandidate) (historyCompactionInputCost, error) {
		cost, err := readHistoryCompactionInputCost(ctx, dir, c)
		cancel()
		return cost, err
	}); !errors.Is(err, context.Canceled) || len(cache.entries) != 0 {
		t.Fatal("canceled fresh remembered", err)
	}
	if got := cache.snapshot(); got.FreshReads != 1 || got.MetadataReads != 2 {
		t.Fatal("actual fresh reader not counted", got)
	}
}

func TestHistoryCostHintsUnknownRefFallbackAndOverflow(t *testing.T) {
	dir := t.TempDir()
	c := costCacheCandidates(t, costCacheLeaf(t, dir, 1, 1))[0]
	cache := newHistoryCompactionCostCache()
	if err := cache.bind(context.Background(), dir, 0); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("unsupported original reader")
	for _, mutate := range []func(*historyCompactionCandidate){func(c *historyCompactionCandidate) { c.history.Dataset = "unknown" }, func(c *historyCompactionCandidate) { c.companions[0].Kind = "unknown" }, func(c *historyCompactionCandidate) { c.history.Checksum = "" }, func(c *historyCompactionCandidate) { c.history.Size = 0 }} {
		bad := c
		bad.companions = append([]SegmentRef(nil), c.companions...)
		mutate(&bad)
		for _, fresh := range []bool{false, true} {
			calls := 0
			reader := func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error) {
				calls++
				return historyCompactionInputCost{bytes: 7}, failure
			}
			var cost historyCompactionInputCost
			var err error
			if fresh {
				cost, err = cache.readFresh(context.Background(), bad, reader)
			} else {
				cost, err = cache.read(context.Background(), bad, reader)
			}
			if calls != 1 || !errors.Is(err, failure) || cost.bytes != 7 || len(cache.entries) != 0 {
				t.Fatal("fallback changed original reader result", cost, err, calls)
			}
		}
	}
	for _, dimension := range []string{"bytes", "logical", "records"} {
		t.Run(dimension, func(t *testing.T) {
			costs := []historyCompactionInputCost{{}, {}, {}}
			for i, n := range []uint64{^uint64(0) - 2, 1, 5} {
				switch dimension {
				case "bytes":
					costs[i].bytes = n
				case "logical":
					costs[i].logicalBytes = n
				case "records":
					costs[i].records = n
				}
			}
			candidates, read := budgetTestLeaves(costs...)
			sel, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{MaxSources: 3}, read)
			if err != nil || !ok || len(sel.candidates) != 2 || sel.toTxNum != 2 {
				t.Fatal("aggregate overflow crossed group boundary", sel, ok, err)
			}
		})
	}
}
