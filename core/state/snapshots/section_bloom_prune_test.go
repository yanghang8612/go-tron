package snapshots

import (
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPruneHotSectionBloomsKeepsColdReads(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	rowA := sectionBloomTestEncodedBit(t, 5)
	rowB := sectionBloomTestEncodedBit(t, 9)
	if err := rawdb.WriteSectionBloom(db, 0, 42, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}
	if err := rawdb.WriteSectionBloom(db, 1, 99, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 1/99: %v", err)
	}

	ref, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection*2-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	result, err := PruneHotSectionBlooms(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotSectionBlooms: %v", err)
	}
	if !result.HasRange || result.FromSection != 0 || result.ToSection != 1 ||
		result.ColdBloomSegments != 1 || result.RowsDeleted != 2 {
		t.Fatalf("prune result = %+v, want sections 0..1 and two rows", result)
	}
	if got := rawdb.ReadSectionBloom(db, 0, 42); got != nil {
		t.Fatalf("hot ReadSectionBloom after prune = %x, want nil", got)
	}

	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	db.SetSectionBloomReader(mgr)
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(db, 1, 99)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 9) {
		t.Fatalf("cold ReadSectionBloomBitSet after prune = %x/%v/%v, want bit 9", bitset, ok, err)
	}
}

func TestPruneHotSectionBloomsRejectsColdMismatchBeforeDeleting(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	original := sectionBloomTestEncodedBit(t, 5)
	changed := sectionBloomTestEncodedBit(t, 6)
	if err := rawdb.WriteSectionBloom(db, 0, 42, original); err != nil {
		t.Fatalf("WriteSectionBloom original: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := rawdb.WriteSectionBloom(db, 0, 42, changed); err != nil {
		t.Fatalf("WriteSectionBloom changed: %v", err)
	}

	_, err = PruneHotSectionBlooms(db, snapshotDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "differs from hot row") {
		t.Fatalf("PruneHotSectionBlooms error = %v, want cold/hot mismatch", err)
	}
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(db, 0, 42)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 6) {
		t.Fatalf("hot section bloom after failed prune = %x/%v/%v, want changed bit 6", bitset, ok, err)
	}
}

func TestPruneHotSectionBloomsWithProgressSkipsProcessedBlocks(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	rowA := sectionBloomTestEncodedBit(t, 5)
	rowB := sectionBloomTestEncodedBit(t, 9)
	if err := rawdb.WriteSectionBloom(db, 0, 42, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}
	if err := rawdb.WriteSectionBloom(db, 1, 99, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 1/99: %v", err)
	}
	refA, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB A: %v", err)
	}
	refB, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", rawdb.SectionBloomBlockPerSection, rawdb.SectionBloomBlockPerSection*2-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB B: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{refA, refB})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	first, err := PruneHotSectionBloomsWithProgress(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("first PruneHotSectionBloomsWithProgress: %v", err)
	}
	if !first.HasRange || first.FromSection != 0 || first.ToSection != 1 ||
		first.ColdBloomSegments != 2 || first.RowsDeleted != 2 {
		t.Fatalf("first prune result = %+v, want sections 0..1 with two rows", first)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || got != rawdb.SectionBloomBlockPerSection*2-1 {
		t.Fatalf("section bloom prune stage = %d ok=%v err=%v, want %d", got, ok, err, rawdb.SectionBloomBlockPerSection*2-1)
	}

	second, err := PruneHotSectionBloomsWithProgress(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("second PruneHotSectionBloomsWithProgress: %v", err)
	}
	if second.HasRange || second.ColdBloomSegments != 0 || second.RowsDeleted != 0 {
		t.Fatalf("second prune result = %+v, want no repeated work", second)
	}
}

func TestSectionBloomPruneLifecycleOnePass(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	row := sectionBloomTestEncodedBit(t, 5)
	if err := rawdb.WriteSectionBloom(db, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	lifecycle := NewSectionBloomPruneLifecycle(db, SectionBloomPruneLifecycleConfig{Dir: snapshotDir})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result == nil || !result.HasRange || result.RowsDeleted != 1 {
		t.Fatalf("OnePass result = %+v, want one deleted row", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || got != rawdb.SectionBloomBlockPerSection-1 {
		t.Fatalf("section bloom prune stage = %d ok=%v err=%v, want %d", got, ok, err, rawdb.SectionBloomBlockPerSection-1)
	}
}

func TestSectionBloomPruneLifecycleNoManifestNoop(t *testing.T) {
	lifecycle := NewSectionBloomPruneLifecycle(rawdb.NewMemoryChainDB(), SectionBloomPruneLifecycleConfig{Dir: t.TempDir()})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result != nil {
		t.Fatalf("OnePass result = %+v, want nil without manifest", result)
	}
}
