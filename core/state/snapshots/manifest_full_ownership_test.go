package snapshots

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestManifestPublicationPackedStringsRoundTrip(t *testing.T) {
	// Cross many block boundaries, include empty fields and a field larger
	// than one block. Publication must own all strings with identical bytes.
	m := cachedManifestFixture(320)
	for i := range m.Segments {
		m.Segments[i].Path += strings.Repeat("世界<&>\"", i%41)
	}
	m.Retired[0].Path = "retired/" + strings.Repeat("a", 100<<10)
	m.Segments[2].Checksum = ""
	want := cloneDecodedManifest(m)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var cache manifestDecodeCache
	cache.retainPublished(raw, m, manifestCacheConfigMaxBytes)
	if cache.manifest == nil || !reflect.DeepEqual(cache.manifest, want) {
		t.Fatal("packing changed string content")
	}
	m.Segments[0].Path, m.Retired[0].Path = "changed", "changed"
	m.Chain.GenesisHash = "changed"
	if !reflect.DeepEqual(cache.manifest, want) {
		t.Fatal("packed generation aliases public mutable containers")
	}
}

func TestManifestReadOnlyDecodeOwnsActualCapacity(t *testing.T) {
	raw, err := json.Marshal(cachedManifestFixture(31))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProductionManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	actual := manifestCacheCharge(raw, decoded)
	compact := manifestCacheCharge(raw, cloneDecodedManifest(decoded))
	if actual <= compact {
		t.Fatal("fixture lacks decoder capacity slack")
	}
	for _, budget := range []uint64{actual, actual - 1, compact} {
		var cache manifestDecodeCache
		got, err := cache.decodeMode(bytes.Clone(raw), true, budget, true)
		if err != nil || !reflect.DeepEqual(got, decoded) {
			t.Fatal("read-only decode changed result", err)
		}
		if budget < actual {
			if cache.manifest != nil || cache.charge != 0 {
				t.Fatal("decoder spare capacity escaped the budget")
			}
			continue
		}
		if cache.manifest != got || cache.charge != manifestCacheCharge(cache.data, got) {
			t.Fatal("decoder ownership was not transferred with exact capacity charge")
		}
		public, err := cache.decodeOwned(bytes.Clone(raw), true, budget)
		if err != nil || !reflect.DeepEqual(public, got) {
			t.Fatal("public load differs from borrowed generation", err)
		}
		public.Segments[0].Path, public.Retired[0].Path = "changed", "changed"
		public.Chain.GenesisHash = "changed"
		if !reflect.DeepEqual(got, decoded) {
			t.Fatal("public hit aliases transferred immutable generation")
		}
	}
}

func TestManifestReadOnlyRestartPublicationIsolation(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, strconv.Itoa(manifestCacheDefaultBytes))
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)
	dir := t.TempDir()
	m := manifestValidationFixture(128)
	m.Progress = &Progress{HotPruneTxNum: 1}
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	loadedManifestCache.clear()
	borrowed, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := cloneDecodedManifest(borrowed)
	if err := UpdateHotPruneProgress(dir, 2, 2); err != nil {
		t.Fatal(err)
	}
	m.Generation++
	m.Segments[0].Size++
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(borrowed, want) {
		t.Fatal("publication mutated the transferred old generation")
	}
	// A same-length replacement is authenticated even after ownership transfer.
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	replaced := bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":9`), 1)
	if bytes.Equal(replaced, raw) || len(replaced) != len(raw) {
		t.Fatal("replacement fixture did not preserve length")
	}
	if err := os.WriteFile(filepath.Join(dir, ManifestFile), replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductionManifestReadOnly(dir); err == nil {
		t.Fatal("transferred cache masked invalid external replacement")
	}
	if !reflect.DeepEqual(borrowed, want) {
		t.Fatal("external replacement changed an old borrowed generation")
	}
}

func TestManifestPublicationStringCapacityBudget(t *testing.T) {
	m := cachedManifestFixture(320)
	for i := range m.Segments {
		m.Segments[i].Checksum = strings.Repeat("界", 1+i%317)
	}
	m.Retired[0].Path = "retired/" + strings.Repeat("x", 100<<10)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	owned := cloneDecodedManifest(m)
	slack := clonePublishedManifestStrings(owned)
	if slack == 0 {
		t.Fatal("fixture did not exercise block allocation slack")
	}
	logical := manifestCacheCharge(raw, owned)
	actual := manifestCacheChargeWithStringSlack(raw, owned, slack)
	for _, budget := range []uint64{logical, actual - 1, actual} {
		var cache manifestDecodeCache
		cache.retainPublished(bytes.Clone(raw), m, budget)
		if budget < actual {
			if cache.manifest != nil || cache.charge != 0 || cache.stringSlack != 0 {
				t.Fatal("post-construction budget rejection retained an arena")
			}
		} else if cache.manifest == nil || cache.charge != actual || cache.stringSlack != slack {
			t.Fatal("admission did not charge all measured block capacities")
		}
	}
	if manifestCacheChargeWithStringSlack(raw, owned, math.MaxUint64) != math.MaxUint64 {
		t.Fatal("string-capacity charge overflowed")
	}
}

func TestManifestProgressPreservesStringCapacityCharge(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, strconv.Itoa(manifestCacheDefaultBytes))
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)
	dir := t.TempDir()
	m := cachedManifestFixture(320)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	loadedManifestCache.mu.Lock()
	before := loadedManifestCache.stringSlack
	loadedManifestCache.mu.Unlock()
	if before == 0 {
		t.Fatal("fixture did not retain string capacity slack")
	}
	if err := UpdateHotPruneProgress(dir, 9, 9); err != nil {
		t.Fatal(err)
	}
	loadedManifestCache.mu.Lock()
	defer loadedManifestCache.mu.Unlock()
	cache := &loadedManifestCache
	if cache.stringSlack != before || cache.charge != manifestCacheChargeWithStringSlack(cache.data, cache.manifest, before) {
		t.Fatal("progress generation dropped shared string block capacity charge")
	}
}
