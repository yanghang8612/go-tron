package snapshots

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func TestManifestPublicationSeedsExactDecodedView(t *testing.T) {
	for _, shape := range []string{"nil", "empty", "normalized", "escaped", "legacy"} {
		t.Run(shape, func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			t.Setenv(manifestCacheBudgetEnv, "")
			loadedManifestCache.clear()
			m := &Manifest{}
			switch shape {
			case "empty":
				m.Segments, m.Retired = []SegmentRef{}, []SegmentRef{}
			case "normalized":
				m = cachedManifestFixture(4)
				m.Version, m.Generation, m.PublishedUnix = 0, 0, 0
				m.Chain.GenesisHash = " 0X" + strings.Repeat("A", 64) + " "
				m.Chain.ForkConfigHash = " SHA256:" + strings.Repeat("B", 64) + " "
			case "legacy":
				m = NewManifest(1, 2, []SegmentRef{{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory,
					FromTxNum: 1, ToTxNum: 2, Path: "history/legacy.json"}})
			case "escaped":
				m = NewManifest(1, 2, []SegmentRef{{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest,
					FromTxNum: 1, ToTxNum: 2, Path: "latest/转义-<>&\"\u2028.json"}})
			}
			seeds, hits, publicationHits := manifestCachePublicationSeeds.Snapshot().Count(), manifestCacheHits.Snapshot().Count(), manifestCachePublicationHits.Snapshot().Count()
			dir := t.TempDir()
			if err := PublishManifest(dir, m); err != nil {
				t.Fatal(err)
			}
			if manifestCachePublicationSeeds.Snapshot().Count() != seeds+1 || manifestCacheHits.Snapshot().Count() != hits {
				t.Fatal("publication was not seeded separately from read hits")
			}
			raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			want, err := decodeManifest(raw)
			if err != nil {
				t.Fatal(err)
			}
			// Both the publisher and every loader keep independently mutable views.
			if m.Chain != nil {
				m.Chain.GenesisHash = "changed after publication"
				m.Progress.LatestBuildTxNum++
			}
			if len(m.Segments) != 0 {
				m.Segments[0].Path = "changed after publication"
			}
			if len(m.Retired) != 0 {
				m.Retired[0].Checksum = "changed after publication"
			}
			misses := manifestCacheMisses.Snapshot().Count()
			got, err := LoadManifest(dir)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("seed differs from JSON round trip: got=%+v want=%+v err=%v", got, want, err)
			}
			if manifestCacheMisses.Snapshot().Count() != misses || manifestCachePublicationHits.Snapshot().Count() != publicationHits+1 {
				t.Fatal("first read decoded an already validated publication")
			}
			got.Generation++
			production, productionErr := LoadProductionManifest(dir)
			wantProduction, wantErr := decodeProductionManifest(raw)
			if (productionErr == nil) != (wantErr == nil) || productionErr != nil && productionErr.Error() != wantErr.Error() || !reflect.DeepEqual(production, wantProduction) {
				t.Fatalf("production validation changed: %v, want %v", productionErr, wantErr)
			}
		})
	}
}

func TestManifestPublicationInvalidUTF8UsesDecoder(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	m := NewManifest(1, 2, []SegmentRef{{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest,
		FromTxNum: 1, ToTxNum: 2, Path: "latest/invalid-\xff.json"}})
	seeds := manifestCachePublicationSeeds.Snapshot().Count()
	dir := t.TempDir()
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if manifestCachePublicationSeeds.Snapshot().Count() != seeds {
		t.Fatal("cached pre-JSON invalid UTF-8 strings")
	}
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	want, err := decodeProductionManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(got, want) || got.Segments[0].Path == m.Segments[0].Path {
		t.Fatalf("invalid UTF-8 round trip changed: %v", err)
	}
}

func TestManifestPublicationReusesIdenticalCachedBytes(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	dir := t.TempDir()
	m := cachedManifestFixture(4)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	seeds, misses := manifestCachePublicationSeeds.Snapshot().Count(), manifestCacheMisses.Snapshot().Count()
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	got, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatalf("identical publication changed its decoded view: %v", err)
	}
	if manifestCachePublicationSeeds.Snapshot().Count() != seeds || manifestCacheMisses.Snapshot().Count() != misses {
		t.Fatal("idempotent publication needlessly replaced or decoded its cached metadata")
	}
}

func TestManifestPublicationUTF8ReplacementPreservesValidationError(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	// Distinct source paths become the same JSON-decoded path after invalid
	// UTF-8 replacement. Publication's valid source cannot replace validation
	// of the transformed bytes that the next loader actually observes.
	m := NewManifest(1, 2, []SegmentRef{
		{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 1, Path: "latest/invalid-\xff.json"},
		{Dataset: SegmentDatasetAccountLatest, Kind: SegmentLatest, FromTxNum: 2, ToTxNum: 2, Path: "latest/invalid-\xfe.json"},
	})
	dir := t.TempDir()
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(dir); err == nil || !strings.Contains(err.Error(), "duplicate segment path") {
		t.Fatalf("source validation concealed the encoded-byte validation error: %v", err)
	}
}

func TestManifestPublicationCacheConfigurationDoesNotFailPublish(t *testing.T) {
	for _, config := range []struct{ toggle, budget string }{{"invalid", ""}, {"1", "invalid"}, {"0", ""}, {"1", "0"}, {"1", "1"}} {
		t.Run(config.toggle+"/"+config.budget, func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", config.toggle)
			t.Setenv(manifestCacheBudgetEnv, config.budget)
			loadedManifestCache.clear()
			seeds := manifestCachePublicationSeeds.Snapshot().Count()
			dir := t.TempDir()
			if err := PublishManifest(dir, cachedManifestFixture(2)); err != nil {
				t.Fatalf("cache setting changed publication result: %v", err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			want, err := decodeProductionManifest(raw)
			if err != nil || manifestCachePublicationSeeds.Snapshot().Count() != seeds {
				t.Fatalf("publication was invalid or unexpectedly seeded: %v", err)
			}
			got, err := LoadProductionManifest(dir)
			if config.toggle == "invalid" || config.budget == "invalid" {
				if err == nil {
					t.Fatal("loader suppressed its configuration error")
				}
			} else if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("disabled/over-budget load result changed: %v", err)
			}
		})
	}
}

func TestManifestPublicationSeedBudgetOwnsStrings(t *testing.T) {
	m := cachedManifestFixture(2)
	backing := strings.Repeat("x", 1<<20) + m.Segments[0].Path
	m.Segments[0].Path = backing[1<<20:]
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	charge := manifestCacheCharge(raw, cloneDecodedManifest(m))
	var cache manifestDecodeCache
	cache.retainPublished(raw, m, charge)
	if cache.manifest == nil || cache.charge != charge || manifestCacheCharge(cache.data, cache.manifest) != charge {
		t.Fatal("publication seed changed exact budget accounting")
	}
	if unsafe.StringData(cache.manifest.Segments[0].Path) == unsafe.StringData(m.Segments[0].Path) ||
		unsafe.StringData(cache.manifest.Chain.GenesisHash) == unsafe.StringData(m.Chain.GenesisHash) {
		t.Fatal("publication seed retained uncharged caller string backing")
	}
	input := bytes.Clone(raw)
	cache.retainPublished(input, m, manifestCacheCharge(input, cloneDecodedManifest(m))-1)
	if cache.manifest != nil || cache.charge != 0 || cache.publication {
		t.Fatal("over-budget publication retained its prior cache entry")
	}
}

func TestManifestFailedPublicationDoesNotSeed(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	seeds := manifestCachePublicationSeeds.Snapshot().Count()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ManifestFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := PublishManifest(dir, cachedManifestFixture(2)); err == nil {
		t.Fatal("rename onto a directory unexpectedly succeeded")
	}
	if manifestCachePublicationSeeds.Snapshot().Count() != seeds {
		t.Fatal("failed publication seeded the cache")
	}
}
