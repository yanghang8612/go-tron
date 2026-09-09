package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestManifestCacheConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, toggle, limit string
		want                uint64
		invalid             bool
	}{
		{name: "default", want: 256 << 20},
		{name: "explicit", toggle: "1", limit: "67108864", want: 64 << 20},
		{name: "whitespace", toggle: " 1 ", limit: " 268435456 ", want: 256 << 20},
		{name: "maximum", limit: "1073741824", want: 1 << 30},
		{name: "one-byte", limit: "1", want: 1},
		{name: "zero-budget", limit: "0"},
		{name: "switch-disabled", toggle: "0", limit: "268435456"},
		{name: "bad-switch", toggle: "yes", invalid: true},
		{name: "sign", limit: "+100", invalid: true},
		{name: "negative", limit: "-1", invalid: true},
		{name: "units", limit: "256MiB", invalid: true},
		{name: "hex", limit: "0x100", invalid: true},
		{name: "exponent", limit: "1e8", invalid: true},
		{name: "fraction", limit: "1.5", invalid: true},
		{name: "separator", limit: "1_000", invalid: true},
		{name: "above-max", limit: "1073741825", invalid: true},
		{name: "overflow", limit: "18446744073709551616", invalid: true},
		{name: "disabled-bad-limit", toggle: "0", limit: "invalid", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", tc.toggle)
			t.Setenv(manifestCacheBudgetEnv, tc.limit)
			got, err := manifestCacheConfiguredBudget()
			if (err != nil) != tc.invalid || err == nil && got != tc.want {
				t.Fatalf("budget=%d err=%v; want %d invalid=%v", got, err, tc.want, tc.invalid)
			}
		})
	}
}

func TestManifestCacheChargesOwnedCompactCapacity(t *testing.T) {
	raw, err := json.Marshal(cachedManifestFixture(31))
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.Clone(raw)
	decoded, err := decodeProductionManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	compact := cloneDecodedManifest(decoded)
	charge := manifestCacheCharge(input, compact)
	if old := manifestCacheCharge(input, decoded); old <= charge {
		t.Fatalf("fixture has no decoder slack: decoded charge %d compact %d", old, charge)
	}
	var cache manifestDecodeCache
	got, err := cache.decodeOwned(input, true, charge)
	if err != nil || !reflect.DeepEqual(got, decoded) {
		t.Fatalf("exact-budget admission changed result: %v", err)
	}
	if cache.manifest == nil || cache.charge != charge || manifestCacheCharge(cache.data, cache.manifest) != charge {
		t.Fatalf("owned charge mismatch: got %d want %d", cache.charge, charge)
	}
	if cap(cache.manifest.Segments) != len(cache.manifest.Segments) || cap(cache.manifest.Retired) != len(cache.manifest.Retired) {
		t.Fatal("cache retains unused decoded capacity")
	}
	got.Segments[0].Checksum = "caller mutation"
	got.Retired[0].Path = "caller mutation"
	hits := manifestCacheHits.Snapshot().Count()
	warm, err := cache.decodeOwned(bytes.Clone(raw), true, charge)
	if err != nil || !reflect.DeepEqual(warm, decoded) || manifestCacheHits.Snapshot().Count() != hits+1 {
		t.Fatalf("compact cache hit changed result: %v", err)
	}
	if manifestCacheCandidate.Snapshot().Value() != int64(charge) || manifestCacheHeadroom.Snapshot().Value() != 0 {
		t.Fatal("candidate/headroom does not report exact admission boundary")
	}
}

func TestManifestCacheRejectReplacementReleasesOldEntry(t *testing.T) {
	small, _ := json.Marshal(cachedManifestFixture(1))
	large, _ := json.Marshal(cachedManifestFixture(40))
	var cache manifestDecodeCache
	if _, err := cache.decodeOwned(bytes.Clone(small), true, manifestCacheDefaultBytes); err != nil {
		t.Fatal(err)
	}
	budget := cache.charge + 1024 // Old entry still fits: rejection must evict it.
	rejected := manifestCacheRejections.Snapshot().Count()
	want, err := decodeProductionManifest(large)
	if err != nil {
		t.Fatal(err)
	}
	got, err := cache.decodeOwned(bytes.Clone(large), true, budget)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("oversized replacement changed result: %v", err)
	}
	if cache.manifest != nil || cache.data != nil || cache.productionErr != nil || cache.charge != 0 {
		t.Fatal("oversized replacement left the old entry resident")
	}
	charge := manifestCacheCandidate.Snapshot().Value()
	if charge <= int64(budget) || manifestCacheRejectedCharge.Snapshot().Value() != charge || manifestCacheRejections.Snapshot().Count() != rejected+1 {
		t.Fatal("budget rejection not observable")
	}
	if manifestCacheResident.Snapshot().Value() != 0 || manifestCacheBudget.Snapshot().Value() != int64(budget) || manifestCacheHeadroom.Snapshot().Value() != int64(budget) {
		t.Fatal("rejection retained resident charge/headroom")
	}
}

func TestManifestCacheLowerBudgetBeforeReadFailure(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	path := filepath.Join(dir, ManifestFile)
	raw, _ := json.Marshal(cachedManifestFixture(4))
	for _, limit := range []string{"0", "1"} {
		t.Setenv(manifestCacheBudgetEnv, "")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadProductionManifest(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		t.Setenv(manifestCacheBudgetEnv, limit)
		if _, err := LoadManifest(dir); !os.IsNotExist(err) {
			t.Fatalf("missing current bytes did not fail: %v", err)
		}
		loadedManifestCache.mu.Lock()
		empty := loadedManifestCache.manifest == nil && loadedManifestCache.data == nil && loadedManifestCache.charge == 0
		loadedManifestCache.mu.Unlock()
		if !empty {
			t.Fatal("failed read retained entry above the newly configured budget")
		}
	}
}

func TestManifestCacheConcurrentBudgetChanges(t *testing.T) {
	raw, _ := json.Marshal(cachedManifestFixture(3))
	var cache manifestDecodeCache
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for worker := range 4 {
		wg.Go(func() {
			for i := range 24 {
				budget := uint64(manifestCacheDefaultBytes)
				if (worker+i)%3 == 0 {
					budget = 1
				}
				m, err := cache.decodeOwned(bytes.Clone(raw), true, budget)
				if err != nil {
					errs <- err
					return
				}
				if m.Generation != 7 || m.Segments[0].Size != 1024 || m.Progress.LatestBuildTxNum != 7 {
					errs <- fmt.Errorf("concurrent budget change exposed mutable cached view")
					return
				}
				m.Segments[0].Size++
				m.Progress.LatestBuildTxNum++
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if _, err := cache.decodeOwned(bytes.Clone(raw), true, 1); err != nil || cache.manifest != nil {
		t.Fatalf("final budget shrink retained a cache: %v", err)
	}
}

// The production-sized catalog uses the observed active/retired counts, with
// synthetic paths and checksums. It does not open or claim to model segment I/O.
func cachedManifestLargeCatalogFixture(longPaths bool) *Manifest {
	m := cachedManifestFixture(5177)
	m.VisibleTxEnd += 10
	for i, kind := range []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor} {
		m.Segments = append(m.Segments, SegmentRef{
			Dataset: SegmentDatasetStateDomainChange, Kind: kind, FromTxNum: 51770, ToTxNum: 51779,
			Path: fmt.Sprintf("history/state-domain-change-51770-51779.%s", []string{"seg", "idx", "kv"}[i]),
			Size: 1024, Checksum: "sha256:" + strings.Repeat("b", 64),
		})
	}
	m.Retired = make([]SegmentRef, 35532)
	if longPaths {
		for i := range m.Segments {
			m.Segments[i].Path = strings.Repeat("snapshots/", 4) + m.Segments[i].Path
		}
	}
	for i := range m.Retired {
		m.Retired[i] = m.Segments[i%len(m.Segments)]
		m.Retired[i].Path = fmt.Sprintf("retired/%08d/%s", i, m.Retired[i].Path)
	}
	return m
}

func TestManifestCacheLargeCatalogCrossesLegacyLimit(t *testing.T) {
	// Added path components deliberately exercise a larger charge. Only the
	// reference counts come from production; these are not production contents.
	raw, err := json.Marshal(cachedManifestLargeCatalogFixture(true))
	if err != nil {
		t.Fatal(err)
	}
	var cache manifestDecodeCache
	want, err := cache.decodeOwned(bytes.Clone(raw), true, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if cache.manifest != nil || manifestCacheCandidate.Snapshot().Value() <= 64<<20 {
		t.Fatal("large fixture did not cross the old budget")
	}
	got, err := cache.decodeOwned(bytes.Clone(raw), true, manifestCacheDefaultBytes)
	if err != nil || !reflect.DeepEqual(got, want) || cache.manifest == nil {
		t.Fatalf("large catalog not admitted with default budget: %v", err)
	}
	hits := manifestCacheHits.Snapshot().Count()
	warm, err := cache.decodeOwned(bytes.Clone(raw), true, manifestCacheDefaultBytes)
	if err != nil || !reflect.DeepEqual(warm, want) || manifestCacheHits.Snapshot().Count() != hits+1 {
		t.Fatalf("large catalog not reused: %v", err)
	}
	if len(warm.Segments) != 25888 || len(warm.Retired) != 35532 || cache.charge > manifestCacheDefaultBytes {
		t.Fatal("large catalog counts or bounded charge changed")
	}
	t.Logf("json=%d active=%d retired=%d resident-charge=%d", len(raw), len(warm.Segments), len(warm.Retired), cache.charge)
}
