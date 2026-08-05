package snapshots

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPruneRetiredSegmentFilesDeletesOnlyRetiredFiles(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	retiredRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "retired"))
	manifest := NewManifest(10, 10, activeRefs)
	manifest.Retired = retiredRefs
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	result, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles: %v", err)
	}
	if result.RetiredSegments != len(retiredRefs) || result.FilesDeleted != len(retiredRefs) || result.FilesMissing != 0 || result.FilesSkippedActive != 0 {
		t.Fatalf("result = %+v, want deleted retired refs", result)
	}
	if result.BytesDeleted == 0 {
		t.Fatalf("BytesDeleted = 0, want positive")
	}
	for _, ref := range activeRefs {
		assertFileExists(t, filepath.Join(dir, ref.Path))
	}
	for _, ref := range retiredRefs {
		assertFileMissing(t, filepath.Join(dir, ref.Path))
	}
	if _, err := VerifyManifestFiles(dir, VerifyManifestOptions{}); err != nil {
		t.Fatalf("VerifyManifestFiles active-only after retired prune: %v", err)
	}
	if _, err := VerifyManifestFiles(dir, VerifyManifestOptions{CheckRetired: true}); err == nil {
		t.Fatal("VerifyManifestFiles with CheckRetired unexpectedly succeeded after retired files were deleted")
	}
}

func TestInspectRetiredSegmentFilesReportsPresentFiles(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	retiredRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "retired"))
	manifest := NewManifest(10, 10, activeRefs)
	manifest.Retired = retiredRefs
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	result, err := InspectRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("InspectRetiredSegmentFiles: %v", err)
	}
	if result.RetiredSegments != len(retiredRefs) || result.FilesPresent != len(retiredRefs) || result.FilesMissing != 0 || result.FilesSkippedActive != 0 {
		t.Fatalf("result = %+v, want present retired refs", result)
	}
	if result.BytesPresent == 0 || len(result.PresentFiles) != len(retiredRefs) {
		t.Fatalf("result = %+v, want present bytes and paths", result)
	}
	for _, ref := range retiredRefs {
		assertFileExists(t, filepath.Join(dir, ref.Path))
	}
}

func TestPruneRetiredSegmentFilesSkipsActivePath(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	manifest := NewManifest(10, 10, activeRefs)
	manifest.Retired = []SegmentRef{activeRefs[0]}
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	result, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles: %v", err)
	}
	if result.FilesDeleted != 0 || result.FilesSkippedActive != 1 {
		t.Fatalf("result = %+v, want one active skip", result)
	}
	assertFileExists(t, filepath.Join(dir, activeRefs[0].Path))
}

func TestPruneRetiredSegmentFilesRequiresActivePreflight(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	retiredRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "retired"))
	manifest := NewManifest(10, 10, activeRefs)
	manifest.Retired = retiredRefs
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, activeRefs[0].Path)); err != nil {
		t.Fatalf("remove active segment: %v", err)
	}

	_, err := PruneRetiredSegmentFiles(dir)
	if err == nil || !strings.Contains(err.Error(), "active segment preflight failed") {
		t.Fatalf("PruneRetiredSegmentFiles error = %v, want active preflight failure", err)
	}
	for _, ref := range retiredRefs {
		assertFileExists(t, filepath.Join(dir, ref.Path))
	}
}

func TestPruneRetiredSegmentFilesSkipsActivePreflightWithoutCandidates(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	manifest := NewManifest(10, 10, activeRefs)
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, activeRefs[0].Path)); err != nil {
		t.Fatalf("remove active segment: %v", err)
	}

	result, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles without retired candidates: %v", err)
	}
	if result.RetiredSegments != 0 || result.FilesDeleted != 0 {
		t.Fatalf("result = %+v, want empty no-op", result)
	}
}

func TestPruneRetiredSegmentFilesSkipsActivePreflightAfterCandidatesGone(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	retiredRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "retired"))
	manifest := NewManifest(10, 10, activeRefs)
	manifest.Retired = retiredRefs
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	for _, ref := range retiredRefs {
		if err := os.Remove(filepath.Join(dir, ref.Path)); err != nil {
			t.Fatalf("remove retired segment %q: %v", ref.Path, err)
		}
	}
	if err := os.Remove(filepath.Join(dir, activeRefs[0].Path)); err != nil {
		t.Fatalf("remove active segment: %v", err)
	}

	result, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatalf("PruneRetiredSegmentFiles after candidates gone: %v", err)
	}
	if result.RetiredSegments != len(retiredRefs) || result.FilesMissing != len(retiredRefs) || result.FilesDeleted != 0 {
		t.Fatalf("result = %+v, want all retired files already missing", result)
	}
}

func TestRetiredPruneLifecycleOnePass(t *testing.T) {
	dir := t.TempDir()
	activeRefs := writeCompactionStateDomainChangeSegment(t, dir, 10, 10, binaryStateDomainChange(10, 10, 1, "active"))
	retiredRefs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "retired"))
	manifest := NewManifest(10, 10, activeRefs)
	manifest.Retired = retiredRefs
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	lifecycle := NewRetiredPruneLifecycle(RetiredPruneLifecycleConfig{Dir: dir})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result == nil || result.FilesDeleted != len(retiredRefs) {
		t.Fatalf("OnePass result = %+v, want deleted retired refs", result)
	}
	for _, ref := range activeRefs {
		assertFileExists(t, filepath.Join(dir, ref.Path))
	}
	for _, ref := range retiredRefs {
		assertFileMissing(t, filepath.Join(dir, ref.Path))
	}
}

func TestRetiredPruneLifecycleNoManifestNoop(t *testing.T) {
	lifecycle := NewRetiredPruneLifecycle(RetiredPruneLifecycleConfig{Dir: t.TempDir()})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result != nil {
		t.Fatalf("OnePass result = %+v, want nil without manifest", result)
	}
}

func BenchmarkPruneRetiredSegmentFilesWithoutCandidates(b *testing.B) {
	dir := b.TempDir()
	changes := make([]*rawdb.StateDomainChange, 4_096)
	for i := range changes {
		txNum := uint64(i + 1)
		changes[i] = binaryStateDomainChange(txNum, txNum, 1, "benchmark")
	}
	activeRefs := writeCompactionStateDomainChangeSegment(b, dir, 1, uint64(len(changes)), changes...)
	manifest := NewManifest(1, uint64(len(changes)), activeRefs)
	if err := PublishManifest(dir, manifest); err != nil {
		b.Fatalf("PublishManifest: %v", err)
	}

	b.Run("full-active-preflight", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("candidate-aware-noop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := PruneRetiredSegmentFiles(dir); err != nil {
				b.Fatal(err)
			}
		}
	})
}
