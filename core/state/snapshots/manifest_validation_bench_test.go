package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func manifestValidationMixedFixture() *Manifest {
	// Generation 33683 native catalog counts; paths/checksums/ranges are
	// synthetic. No segment contents are opened by metadata validation.
	const histories, events, retired = 2794, 30908, 92667
	m := manifestValidationFixture(histories)
	for i := histories; i < events; i++ {
		for j, kind := range []SegmentKind{SegmentEventLog, SegmentEventLogIndex} {
			m.Segments = append(m.Segments, SegmentRef{Dataset: SegmentDatasetEventLog, Kind: kind,
				FromTxNum: uint64(i * 10), ToTxNum: uint64(i*10 + 9), Path: fmt.Sprintf("log/event-%d.%s", i, []string{"seg", "idx"}[j])})
		}
	}
	for i := range m.Segments {
		m.Segments[i].Size = 1024
		m.Segments[i].Checksum = "sha256:" + strings.Repeat("b", 64)
	}
	m.Retired = make([]SegmentRef, retired)
	for i := range m.Retired {
		m.Retired[i] = m.Segments[i%len(m.Segments)]
		m.Retired[i].Path = fmt.Sprintf("retired/%08d/%s", i, m.Retired[i].Path)
	}
	m.Generation, m.PublishedUnix = 33683, 1789370000
	sortSegments(m.Segments)
	sortSegments(m.Retired)
	return m
}

func validationBenchmarkManifest(b *testing.B) *Manifest {
	b.Helper()
	path := os.Getenv("GTRON_MANIFEST_VALIDATION_REPLAY")
	if path == "" {
		return manifestValidationMixedFixture()
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	wantSHA := os.Getenv("GTRON_MANIFEST_VALIDATION_REPLAY_SHA256")
	sum := sha256.Sum256(raw)
	if wantSHA == "" || wantSHA != hex.EncodeToString(sum[:]) {
		b.Fatal("frozen manifest SHA256 missing or mismatched")
	}
	m := new(Manifest)
	if err := json.Unmarshal(raw, m); err != nil {
		b.Fatal(err)
	}
	b.Logf("frozen native metadata SHA256=%s bytes=%d generation=%d", wantSHA, len(raw), m.Generation)
	return m
}

// BenchmarkManifestValidationMetadata supports a SHA-pinned real JSON via
// GTRON_MANIFEST_VALIDATION_REPLAY and GTRON_MANIFEST_VALIDATION_REPLAY_SHA256.
// Reading/parsing that input is outside timing. Publish work only writes into
// b.TempDir; no production manifest or segment is changed/opened.
func BenchmarkManifestValidationMetadata(b *testing.B) {
	m := validationBenchmarkManifest(b)
	if err := frozenManifestValidate(m); err != nil {
		b.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		b.Fatal(err)
	}
	if err := frozenValidateProductionHistorySegments(m); err != nil {
		b.Fatal(err)
	}
	if err := m.ValidateProduction(); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"legacy", "candidate"} {
		b.Run("validate/"+mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if mode == "legacy" {
					err = frozenManifestValidate(m)
				} else {
					err = m.Validate()
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(m.Segments)), "active-refs")
			b.ReportMetric(float64(len(m.Retired)), "retired-refs")
		})
	}
	for _, mode := range []string{"legacy", "candidate"} {
		b.Run("publish-first-load/"+mode, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv(manifestCacheBudgetEnv, "268435456")
			loadedManifestCache.clear()
			b.Cleanup(loadedManifestCache.clear)
			candidate := cloneDecodedManifest(m)
			dir := b.TempDir()
			publish := PublishManifest
			if mode == "legacy" {
				publish = frozenPublishValidationManifest
			}
			if err := publish(dir, candidate); err != nil {
				b.Fatal(err)
			}
			seeds, misses := manifestCachePublicationSeeds.Snapshot().Count(), manifestCacheMisses.Snapshot().Count()
			var got *Manifest
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				candidate.Generation++ // Force a genuinely changed publication.
				if err := publish(dir, candidate); err != nil {
					b.Fatal(err)
				}
				var err error
				got, err = LoadProductionManifest(dir)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if !reflect.DeepEqual(got, candidate) {
				b.Fatal("publication or detached first load changed metadata")
			}
			b.ReportMetric(float64(len(m.Segments)), "active-refs")
			b.ReportMetric(float64(len(m.Retired)), "retired-refs")
			b.ReportMetric(float64(manifestCachePublicationSeeds.Snapshot().Count()-seeds)/float64(b.N), "seeds/op")
			b.ReportMetric(float64(manifestCacheMisses.Snapshot().Count()-misses)/float64(b.N), "decodes/op")
		})
	}
}

func TestManifestValidationPublicationMatchesFrozenBytes(t *testing.T) {
	for _, fixture := range []*Manifest{cachedManifestFixture(9), {
		Version: CurrentManifestVersion, Segments: []SegmentRef{}, Retired: nil, PublishedUnix: 1,
	}} {
		old, candidate := cloneDecodedManifest(fixture), cloneDecodedManifest(fixture)
		oldDir, candidateDir := t.TempDir(), t.TempDir()
		if err := frozenPublishValidationManifest(oldDir, old); err != nil {
			t.Fatal(err)
		}
		if err := PublishManifest(candidateDir, candidate); err != nil {
			t.Fatal(err)
		}
		oldJSON, err := os.ReadFile(filepath.Join(oldDir, ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		newJSON, err := os.ReadFile(filepath.Join(candidateDir, ManifestFile))
		if err != nil || !bytes.Equal(oldJSON, newJSON) {
			t.Fatalf("publication bytes changed: %v", err)
		}
		loaded, err := LoadProductionManifest(candidateDir)
		if err != nil || !reflect.DeepEqual(old, loaded) {
			t.Fatalf("first production load changed: %v", err)
		}
	}
}
