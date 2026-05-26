package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestManifestRoundTripIncludesGenerationAndRetired(t *testing.T) {
	dir := t.TempDir()
	manifest := NewManifest(10, 20, []SegmentRef{
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 10,
			ToTxNum:   20,
			Path:      "history/reward-10-20.seg",
		},
	})
	manifest.Generation = 7
	manifest.Retired = []SegmentRef{
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 1,
			ToTxNum:   9,
			Path:      "history/reward-1-9.seg",
			Size:      123,
			Checksum:  "sha256:abc",
		},
	}

	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	loaded, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.Generation != 7 {
		t.Fatalf("generation = %d, want 7", loaded.Generation)
	}
	if len(loaded.Retired) != 1 {
		t.Fatalf("retired segments = %d, want 1", len(loaded.Retired))
	}
	if loaded.Retired[0].Path != "history/reward-1-9.seg" {
		t.Fatalf("retired path = %q", loaded.Retired[0].Path)
	}
}

func TestManifestDefaultsAndNormalizesGeneration(t *testing.T) {
	manifest := NewManifest(1, 1, nil)
	if manifest.Version != CurrentManifestVersion {
		t.Fatalf("version = %d, want %d", manifest.Version, CurrentManifestVersion)
	}
	if manifest.Generation != 1 {
		t.Fatalf("generation = %d, want 1", manifest.Generation)
	}

	dir := t.TempDir()
	manifest.Generation = 0
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if manifest.Generation != 1 {
		t.Fatalf("published generation = %d, want 1", manifest.Generation)
	}

	inverted := NewManifest(2, 1, nil)
	inverted.Generation = 0
	if err := PublishManifest(t.TempDir(), inverted); err == nil {
		t.Fatal("publish accepted inverted visible range")
	}
	if inverted.Generation != 1 {
		t.Fatalf("failed publish generation = %d, want 1", inverted.Generation)
	}
}

func TestLegacyManifestJSONMissingGenerationAndRetiredLoads(t *testing.T) {
	dir := t.TempDir()
	legacy := `{
  "version": 1,
  "publishedUnix": 1,
  "visibleTxStart": 10,
  "visibleTxEnd": 20,
  "segments": [
    {
      "dataset": "state-domain-change",
      "kind": "history",
      "fromTxNum": 10,
      "toTxNum": 20,
      "path": "history/reward-10-20.seg"
    }
  ]
}`
	if err := os.WriteFile(filepath.Join(dir, ManifestFile), []byte(legacy), 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}
	loaded, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load legacy manifest: %v", err)
	}
	if loaded.Generation != 1 {
		t.Fatalf("generation = %d, want 1", loaded.Generation)
	}
	if len(loaded.Retired) != 0 {
		t.Fatalf("retired segments = %d, want 0", len(loaded.Retired))
	}

	var decoded Manifest
	if err := json.Unmarshal([]byte(legacy), &decoded); err != nil {
		t.Fatalf("unmarshal legacy manifest: %v", err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("validate legacy manifest: %v", err)
	}
}

func TestManifestRejectsInvalidRetiredSegments(t *testing.T) {
	tests := []struct {
		name    string
		retired SegmentRef
	}{
		{
			name: "unsafe path",
			retired: SegmentRef{
				Domain:    kvdomains.SystemReward,
				Kind:      SegmentHistory,
				FromTxNum: 1,
				ToTxNum:   2,
				Path:      "../retired.seg",
			},
		},
		{
			name: "inverted range",
			retired: SegmentRef{
				Domain:    kvdomains.SystemReward,
				Kind:      SegmentHistory,
				FromTxNum: 2,
				ToTxNum:   1,
				Path:      "history/retired.seg",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := NewManifest(10, 20, []SegmentRef{
				{
					Domain:    kvdomains.SystemReward,
					Kind:      SegmentHistory,
					FromTxNum: 10,
					ToTxNum:   20,
					Path:      "history/reward-10-20.seg",
				},
			})
			manifest.Retired = []SegmentRef{tt.retired}
			if err := manifest.Validate(); err == nil {
				t.Fatal("invalid retired segment accepted")
			}
		})
	}
}

func TestManifestRetiredSegmentsDoNotAffectActiveOverlap(t *testing.T) {
	manifest := NewManifest(10, 30, []SegmentRef{
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 10,
			ToTxNum:   20,
			Path:      "history/reward-10-20.seg",
		},
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 21,
			ToTxNum:   30,
			Path:      "history/reward-21-30.seg",
		},
	})
	manifest.Retired = []SegmentRef{
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 15,
			ToTxNum:   25,
			Path:      "history/reward-15-25-retired.seg",
		},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("retired overlap rejected: %v", err)
	}
}
