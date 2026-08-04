package snapshots

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPublishedManifestLeaseProtectsResumableGeneration(t *testing.T) {
	dir, identity, _ := writeVerifiableHistoryManifest(t)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	oldManifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("LoadProductionManifest(old): %v", err)
	}
	oldManifest.PublishedUnix = time.Now().Add(-48 * time.Hour).Unix()
	if err := PublishManifest(dir, oldManifest); err != nil {
		t.Fatalf("PublishManifest(old): %v", err)
	}
	oldCatalog, err := PublishSignedSnapshotCatalog(dir, privateKey)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog(old): %v", err)
	}

	current := NewManifestForChain(oldManifest.VisibleTxStart, oldManifest.VisibleTxEnd, nil, identity)
	current.Generation = oldManifest.Generation + 1
	current.Retired = append([]SegmentRef(nil), oldManifest.Segments...)
	if err := PublishManifest(dir, current); err != nil {
		t.Fatalf("PublishManifest(current): %v", err)
	}
	currentCatalog, err := PublishSignedSnapshotCatalog(dir, privateKey)
	if err != nil {
		t.Fatalf("PublishSignedSnapshotCatalog(current): %v", err)
	}
	if currentCatalog.ManifestPath == oldCatalog.ManifestPath {
		t.Fatalf("catalog generation path did not change: %q", currentCatalog.ManifestPath)
	}

	protected, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles(protected): %v", err)
	}
	if protected.FilesDeleted != 0 || protected.FilesSkippedPublished != len(oldManifest.Segments) {
		t.Fatalf("protected prune = %+v, want %d published skips", protected, len(oldManifest.Segments))
	}
	for _, ref := range oldManifest.Segments {
		if _, err := os.Stat(filepath.Join(dir, ref.Path)); err != nil {
			t.Fatalf("leased segment %q: %v", ref.Path, err)
		}
	}

	expired, err := PrunePublishedSnapshotManifests(dir, 1, time.Hour)
	if err != nil {
		t.Fatalf("PrunePublishedSnapshotManifests: %v", err)
	}
	if expired.Deleted != 1 || expired.Retained != 1 || expired.Paths[0] != oldCatalog.ManifestPath {
		t.Fatalf("published prune = %+v, want old generation expired", expired)
	}
	reclaimed, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles(reclaimed): %v", err)
	}
	if reclaimed.FilesDeleted != len(oldManifest.Segments) || reclaimed.FilesSkippedPublished != 0 {
		t.Fatalf("reclaimed prune = %+v, want %d deleted", reclaimed, len(oldManifest.Segments))
	}
}
