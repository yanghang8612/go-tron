package snapshots

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestManifestOrderedPublicationValidationMatchesPublic(t *testing.T) {
	rng := rand.New(rand.NewSource(20261011))
	for trial := 0; trial < 256; trial++ {
		m := manifestValidationFixture(8)
		switch trial % 8 {
		case 1:
			m.Segments = append(m.Segments, m.Segments[rng.Intn(len(m.Segments))])
		case 2:
			m.Segments[rng.Intn(len(m.Segments))].Path = "history/../escape.seg"
		case 3:
			m.Segments[rng.Intn(len(m.Segments))].Kind = "unknown"
		case 4:
			m.Segments[rng.Intn(len(m.Segments))].FromTxNum = 100
		case 5:
			m.Segments[rng.Intn(len(m.Segments))].AggregationSteps = 123
		case 6:
			m.Segments = m.Segments[1:]
		case 7:
			m.Retired = []SegmentRef{{Kind: SegmentHistory, Path: ".."}}
		}
		rng.Shuffle(len(m.Segments), func(i, j int) { m.Segments[i], m.Segments[j] = m.Segments[j], m.Segments[i] })
		publicErr := m.Validate()
		sortSegments(m.Segments)
		orderedErr := m.validate(true)
		if (publicErr == nil) != (orderedErr == nil) {
			t.Fatalf("trial %d: public=%v ordered=%v", trial, publicErr, orderedErr)
		}
		// Once both see the same sorted input, individual errors have the same
		// priority. Overlap family order in the public map is unspecified.
		publicErr = m.Validate()
		if orderedErr != nil && !strings.Contains(orderedErr.Error(), "overlapping") && publicErr.Error() != orderedErr.Error() {
			t.Fatalf("trial %d error differs: %v / %v", trial, publicErr, orderedErr)
		}
	}
	// A containing interval must fail even if later short intervals do not
	// overlap one another. Family boundaries must not cause false positives.
	m := manifestValidationFixture(3)
	for i := range m.Segments {
		if m.Segments[i].Kind == SegmentEventLog && m.Segments[i].FromTxNum == 0 {
			m.Segments[i].ToTxNum = 29
		}
	}
	sortSegments(m.Segments)
	if err := m.validate(true); err == nil || !strings.Contains(err.Error(), "overlapping") {
		t.Fatalf("nested range: %v", err)
	}
}

func TestManifestParentPathScanMatchesOriginal(t *testing.T) {
	paths := []string{"", ".", "..", "/", "//", "a/", "/a", "a//b", "a/./b", "a/../b", "../a", "a/..", "a/.../b", "非ASCII/..", "非ASCII/正常", `a\..\b`, `a\b`, "a/..\\b", "a/..//b"}
	rng := rand.New(rand.NewSource(11))
	parts := []string{"", ".", "..", "...", "非ASCII", "a", `\`, "/"}
	for i := 0; i < 1000; i++ {
		var p strings.Builder
		for j := 0; j < 8; j++ {
			p.WriteString(parts[rng.Intn(len(parts))])
		}
		paths = append(paths, p.String())
	}
	for _, p := range paths {
		want := false
		for _, part := range strings.Split(filepath.ToSlash(p), "/") {
			want = want || part == ".."
		}
		if got := hasParentDir(p); got != want {
			t.Fatalf("path %q: got %v want %v", p, got, want)
		}
	}
}

func TestManifestIntegratedPublicationOwnsInputsAndRestarts(t *testing.T) {
	base := manifestValidationFixture(4)
	base.Retired = append([]SegmentRef(nil), manifestValidationFixture(1).Segments...)
	base.Progress = &Progress{HotPruneTxNum: 2, HotPruneBlockNum: 1}
	refs := append([]SegmentRef(nil), manifestValidationFixture(1).Segments...)
	rng := rand.New(rand.NewSource(123))
	rng.Shuffle(len(base.Segments), func(i, j int) { base.Segments[i], base.Segments[j] = base.Segments[j], base.Segments[i] })
	rng.Shuffle(len(refs), func(i, j int) { refs[i], refs[j] = refs[j], refs[i] })
	baseBefore, refsBefore := cloneDecodedManifest(base), append([]SegmentRef(nil), refs...)
	badDir := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(badDir, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAggregator(badDir).integrateWithManifest(0, 39, refs, base); err == nil {
		t.Fatal("published through a regular file")
	}
	if !reflect.DeepEqual(base, baseBefore) || !reflect.DeepEqual(refs, refsBefore) {
		t.Fatal("failed publication changed caller inputs")
	}
	dir := t.TempDir()
	m, err := NewAggregator(dir).integrateWithManifest(0, 39, refs, base)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(base, baseBefore) || !reflect.DeepEqual(refs, refsBefore) {
		t.Fatal("publication aliases caller inputs")
	}
	if m.Progress.HotPruneTxNum != 2 || m.Progress.HotPruneBlockNum != 1 {
		t.Fatal("integration advanced completed prune progress")
	}
	loadedManifestCache.clear() // Simulate the first load after process restart.
	loaded, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(m, loaded) {
		t.Fatalf("restart changed publication: %v", err)
	}
	oracleDir := t.TempDir()
	if err := frozenPublishValidationManifest(oracleDir, cloneDecodedManifest(m)); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(oracleDir, ManifestFile))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("publication bytes changed: %v", err)
	}
}

func TestManifestEmptyIntegrationPreservesNullSegments(t *testing.T) {
	for _, old := range []*Manifest{nil, NewManifest(0, 0, nil), NewManifest(0, 0, []SegmentRef{})} {
		dir := t.TempDir()
		m, err := NewAggregator(dir).integrateWithManifest(0, 0, []SegmentRef{}, old)
		if err != nil || m.Segments != nil {
			t.Fatalf("empty integration: %v %+v", err, m)
		}
		oracle := NewManifest(0, 0, nil)
		oracle.Generation, oracle.PublishedUnix, oracle.Progress = m.Generation, m.PublishedUnix, cloneProgress(m.Progress)
		oracleDir := t.TempDir()
		if err := frozenPublishValidationManifest(oracleDir, oracle); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(dir, ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(filepath.Join(oracleDir, ManifestFile))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("empty JSON differs: %v\n%s\n%s", err, got, want)
		}
		loadedManifestCache.clear()
		loaded, err := LoadProductionManifest(dir)
		if err != nil || loaded.Segments != nil {
			t.Fatalf("empty restart: %v %+v", err, loaded)
		}
	}
}
