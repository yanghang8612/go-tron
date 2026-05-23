package snapshots

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestPublishLoadManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	manifest := NewManifest(10, 30, []SegmentRef{
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 10,
			ToTxNum:   20,
			Path:      "history/reward-10-20.seg",
			Size:      123,
			Checksum:  "sha256:abc",
		},
		{
			Domain:    kvdomains.SystemReward,
			Kind:      SegmentHistory,
			FromTxNum: 21,
			ToTxNum:   30,
			Path:      "history/reward-21-30.seg",
			Size:      456,
		},
		{
			Domain:    kvdomains.ContractStorage,
			Kind:      SegmentLatest,
			FromTxNum: 10,
			ToTxNum:   30,
			Path:      "latest/storage-10-30.seg",
		},
	})
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFile)); err != nil {
		t.Fatalf("manifest not published: %v", err)
	}
	loaded, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.Version != ManifestVersion || loaded.VisibleTxStart != 10 || loaded.VisibleTxEnd != 30 {
		t.Fatalf("loaded manifest = %+v", loaded)
	}
	if len(loaded.Segments) != 3 {
		t.Fatalf("loaded segments = %d", len(loaded.Segments))
	}
}

func TestManifestRejectsOverlappingVisibleSegments(t *testing.T) {
	manifest := NewManifest(1, 10, []SegmentRef{
		{Domain: kvdomains.SystemReward, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 5, Path: "a.seg"},
		{Domain: kvdomains.SystemReward, Kind: SegmentHistory, FromTxNum: 5, ToTxNum: 10, Path: "b.seg"},
	})
	if err := manifest.Validate(); err == nil {
		t.Fatal("expected overlapping segments to be rejected")
	}
}

func TestManifestRejectsUnsafePaths(t *testing.T) {
	tests := []string{
		"",
		"/abs.seg",
		"../parent.seg",
		"nested/../escape.seg",
		".",
	}
	for _, path := range tests {
		manifest := NewManifest(1, 1, []SegmentRef{
			{Domain: kvdomains.SystemReward, Kind: SegmentLatest, FromTxNum: 1, ToTxNum: 1, Path: path},
		})
		if err := manifest.Validate(); err == nil {
			t.Fatalf("path %q accepted", path)
		}
	}
}
