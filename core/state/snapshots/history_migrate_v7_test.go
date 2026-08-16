package snapshots

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryUsesCurrentCompressionDistinguishesChunkLayout(t *testing.T) {
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("history-compression-sample-"), 10_000)
	currentPath := filepath.Join(dir, "current.seg")
	if err := compressBlobToFile(dir, currentPath, payload, historyCompressChunkSize); err != nil {
		t.Fatalf("write current compression: %v", err)
	}
	current, err := historyUsesCurrentCompression(currentPath)
	if err != nil || !current {
		t.Fatalf("current compression = %t, err=%v", current, err)
	}
	legacyPath := filepath.Join(dir, "legacy.seg")
	if err := compressBlobToFile(dir, legacyPath, payload, 64<<10); err != nil {
		t.Fatalf("write legacy compression: %v", err)
	}
	current, err = historyUsesCurrentCompression(legacyPath)
	if err != nil || current {
		t.Fatalf("legacy compression = %t, err=%v", current, err)
	}
}

func TestMigrateHistoryV7ResumesAtTrioBoundaries(t *testing.T) {
	dir := t.TempDir()
	first := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "first"))
	second := writeCompactionStateDomainChangeSegment(t, dir, 2, 2, binaryStateDomainChange(2, 2, 1, "second"))
	refs := append(append([]SegmentRef{}, first...), second...)
	if err := PublishManifest(dir, NewManifest(1, 2, refs)); err != nil {
		t.Fatalf("publish legacy manifest: %v", err)
	}

	progressCalls := 0
	partial, err := MigrateHistoryV7(dir, HistoryV7MigrationOptions{
		MaxTrios: 1,
		OnProgress: func(progress HistoryV7MigrationProgress) {
			progressCalls++
			if progress.MigratedTrios != 1 || progress.TotalTrios != 2 {
				t.Fatalf("progress = %+v", progress)
			}
		},
	})
	if err != nil {
		t.Fatalf("partial migration: %v", err)
	}
	if partial.TotalTrios != 2 || partial.AlreadyCurrent != 0 || partial.MigratedTrios != 1 || partial.RemainingTrios != 1 || progressCalls != 1 {
		t.Fatalf("partial result = %+v, progressCalls=%d", partial, progressCalls)
	}
	if partial.ActiveBytesBefore == 0 || partial.ActiveBytesAfter == 0 || partial.RetiredBytesAdded == 0 {
		t.Fatalf("partial byte accounting = %+v", partial)
	}

	resumed, err := MigrateHistoryV7(dir, HistoryV7MigrationOptions{})
	if err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if resumed.TotalTrios != 2 || resumed.AlreadyCurrent != 1 || resumed.MigratedTrios != 1 || resumed.RemainingTrios != 0 {
		t.Fatalf("resumed result = %+v", resumed)
	}

	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatalf("load migrated manifest: %v", err)
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	candidates := historyCompactionCandidates(manifest, cfg)
	if len(candidates) != 2 {
		t.Fatalf("active trios = %d, want 2", len(candidates))
	}
	for _, candidate := range candidates {
		current, err := historyCandidateUsesCurrentV7(dir, candidate)
		if err != nil || !current {
			t.Fatalf("current trio %q = %t, err=%v", candidate.history.Path, current, err)
		}
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open migrated manager: %v", err)
	}
	var got []*rawdb.StateDomainChange
	if err := mgr.IterateStateDomainChanges(1, 2, func(change *rawdb.StateDomainChange) (bool, error) {
		got = append(got, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate migrated history: %v", err)
	}
	assertBinaryChangeOrder(t, got, []binaryChangeOrder{
		{txNum: 1, seq: 1, key: "first"},
		{txNum: 2, seq: 1, key: "second"},
	})

	noop, err := MigrateHistoryV7(dir, HistoryV7MigrationOptions{})
	if err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
	if noop.AlreadyCurrent != 2 || noop.MigratedTrios != 0 || noop.RemainingTrios != 0 {
		t.Fatalf("idempotent result = %+v", noop)
	}
}
