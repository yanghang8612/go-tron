package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryReadLeaseDefersRetiredFilePrune(t *testing.T) {
	dir := t.TempDir()
	manager := &Manager{dir: dir}
	pinned, release, err := manager.PinHistoryReadView()
	if err != nil {
		t.Fatal(err)
	}
	if pinned != nil {
		t.Fatal("empty directory unexpectedly has a cold manifest")
	}
	result, err := PruneRetiredSegmentFilesContextWithVerifier(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DeferredForReaders {
		t.Fatal("retired file prune ran while a history reader held its lease")
	}
	if _, ok := tryHistoryRetirement(); ok {
		t.Fatal("compaction retirement entered while a history reader held its lease")
	}
	release()
	release() // release is idempotent, including error/cancellation cleanup.
	if unlock, ok := tryHistoryRetirement(); !ok {
		t.Fatal("retirement remained blocked after the reader closed")
	} else {
		unlock()
	}
}

func TestPinnedColdHistorySurvivesMergeUntilReaderClose(t *testing.T) {
	dir := t.TempDir()
	refs := append([]SegmentRef{}, writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "a"))...)
	refs = append(refs, writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "b"))...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatal(err)
	}
	manager, err := OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	pinned, release, err := manager.PinHistoryReadView()
	if err != nil || pinned == nil {
		t.Fatalf("pin old manifest: pinned=%t err=%v", pinned != nil, err)
	}
	read := func(m *Manager) int {
		t.Helper()
		rows := 0
		if err := m.IterateStateDomainChanges(1, 2, func(_ *rawdb.StateDomainChange) (bool, error) {
			rows++
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	if got := read(pinned); got != 2 {
		t.Fatalf("old pinned rows = %d, want 2", got)
	}
	merged, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{DeleteObsolete: true})
	if err != nil || !merged.Merged {
		t.Fatalf("merge under reader lease: result=%+v err=%v", merged, err)
	}
	for _, ref := range refs {
		if _, err := os.Stat(filepath.Join(dir, ref.Path)); err != nil {
			t.Fatalf("merge removed a pinned input %q: %v", ref.Path, err)
		}
	}
	if got := read(pinned); got != 2 {
		t.Fatalf("old pinned rows after merge = %d, want 2", got)
	}
	deferred, err := PruneRetiredSegmentFiles(dir)
	if err != nil || !deferred.DeferredForReaders {
		t.Fatalf("retired prune while pinned = %+v, %v", deferred, err)
	}
	release()
	if got := read(manager); got != 2 {
		t.Fatalf("new manifest rows = %d, want 2", got)
	}
	reclaimed, err := PruneRetiredSegmentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.FilesDeleted == 0 {
		t.Fatalf("old inputs were not reclaimed after release: %+v", reclaimed)
	}
}

func TestHistoryReadLeaseRejectsConcurrentRetirement(t *testing.T) {
	unlock, ok := tryHistoryRetirement()
	if !ok {
		t.Fatal("retirement gate unexpectedly busy")
	}
	defer unlock()
	_, release, err := (&Manager{dir: t.TempDir()}).PinHistoryReadView()
	if !errors.Is(err, ErrHistoryReadLeaseBusy) || release != nil {
		t.Fatalf("pin during retirement = err %v, has lease %t; want busy and no lease", err, release != nil)
	}
}
