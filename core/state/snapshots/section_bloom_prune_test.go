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

func TestPruneHotSectionBloomsCountsOnlyHotRowsDeleted(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	rowA := sectionBloomTestEncodedBit(t, 5)
	rowB := sectionBloomTestEncodedBit(t, 9)
	if err := rawdb.WriteSectionBloom(db, 0, 42, rowA); err != nil {
		t.Fatalf("WriteSectionBloom 0/42: %v", err)
	}
	if err := rawdb.WriteSectionBloom(db, 0, 99, rowB); err != nil {
		t.Fatalf("WriteSectionBloom 0/99: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := rawdb.DeleteSectionBloom(db, 0, 99); err != nil {
		t.Fatalf("DeleteSectionBloom 0/99: %v", err)
	}

	result, err := PruneHotSectionBlooms(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotSectionBlooms: %v", err)
	}
	if !result.HasRange || result.FromSection != 0 || result.ToSection != 0 ||
		result.ColdBloomSegments != 1 || result.RowsDeleted != 1 {
		t.Fatalf("prune result = %+v, want one actual hot row deleted", result)
	}
	if got := rawdb.ReadSectionBloom(db, 0, 42); got != nil {
		t.Fatalf("hot ReadSectionBloom 0/42 after prune = %x, want nil", got)
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
	blockA := canonicalBoundaryTestBlock(t, rawdb.SectionBloomBlockPerSection-1)
	if err := rawdb.WriteBlock(db, blockA); err != nil {
		t.Fatalf("WriteBlock section A end: %v", err)
	}
	blockB := canonicalBoundaryTestBlock(t, rawdb.SectionBloomBlockPerSection*2-1)
	if err := rawdb.WriteBlock(db, blockB); err != nil {
		t.Fatalf("WriteBlock section B end: %v", err)
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
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != blockB.Hash() {
		t.Fatalf("section bloom prune stage row = %+v ok=%v err=%v, want block %d hash %x", row, ok, err, blockB.Number(), blockB.Hash())
	}

	second, err := PruneHotSectionBloomsWithProgress(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("second PruneHotSectionBloomsWithProgress: %v", err)
	}
	if second.HasRange || second.ColdBloomSegments != 0 || second.RowsDeleted != 0 {
		t.Fatalf("second prune result = %+v, want no repeated work", second)
	}
}

func TestPruneHotSectionBloomsWithProgressUpgradesUnboundResumeStage(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	row := sectionBloomTestEncodedBit(t, 5)
	if err := rawdb.WriteSectionBloom(db, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	block := canonicalBoundaryTestBlock(t, rawdb.SectionBloomBlockPerSection-1)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock section end: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(db, snapshotDir, "", 0, rawdb.SectionBloomBlockPerSection-1)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotSectionBloomPrune, rawdb.SectionBloomBlockPerSection-1); err != nil {
		t.Fatalf("WriteStageProgress SnapshotSectionBloomPrune: %v", err)
	}

	result, err := PruneHotSectionBloomsWithProgress(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotSectionBloomsWithProgress: %v", err)
	}
	if !result.HasRange || result.RowsDeleted != 1 {
		t.Fatalf("prune result = %+v, want reprocessed section bloom row", result)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("upgraded section bloom prune row = %+v ok=%v err=%v, want block hash %x", row, ok, err, block.Hash())
	}
}

func TestPruneHotSectionBloomsWithProgressRejectsResumeHashMismatchBeforeDelete(t *testing.T) {
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
	blockA := canonicalBoundaryTestBlock(t, rawdb.SectionBloomBlockPerSection-1)
	if err := rawdb.WriteBlock(db, blockA); err != nil {
		t.Fatalf("WriteBlock section A end: %v", err)
	}
	blockB := canonicalBoundaryTestBlock(t, rawdb.SectionBloomBlockPerSection*2-1)
	if err := rawdb.WriteBlock(db, blockB); err != nil {
		t.Fatalf("WriteBlock section B end: %v", err)
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
	wrongHash := blockA.Hash()
	wrongHash[0] ^= 0xff
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotSectionBloomPrune, blockA.Number(), wrongHash); err != nil {
		t.Fatalf("WriteStageProgressWithHash SnapshotSectionBloomPrune: %v", err)
	}

	_, err = PruneHotSectionBloomsWithProgress(db, snapshotDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "does not match canonical hash") {
		t.Fatalf("PruneHotSectionBloomsWithProgress resume mismatch err = %v, want canonical hash mismatch", err)
	}
	bitset, ok, err := rawdb.ReadSectionBloomBitSet(db, 1, 99)
	if err != nil || !ok || !rawdb.SectionBloomBitSetHas(bitset, 9) {
		t.Fatalf("hot section bloom after rejected resume mismatch = %x/%v/%v, want bit 9", bitset, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != wrongHash {
		t.Fatalf("section bloom prune row after rejected resume mismatch = %+v ok=%v err=%v, want original wrong hash %x", row, ok, err, wrongHash)
	}
}

func TestPruneHotSectionBloomsWithProgressUsesAncientBlockHash(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	hot := rawdb.NewMemoryDatabase()
	row := sectionBloomTestEncodedBit(t, 5)
	if err := rawdb.WriteSectionBloom(hot, 0, 42, row); err != nil {
		t.Fatalf("WriteSectionBloom: %v", err)
	}
	sectionEnd := uint64(rawdb.SectionBloomBlockPerSection - 1)
	block := canonicalBoundaryTestBlock(t, sectionEnd)
	if err := rawdb.WriteBlock(hot, block); err != nil {
		t.Fatalf("WriteBlock section end: %v", err)
	}
	ref, err := BuildSectionBloomSegmentFromDB(hot, snapshotDir, "", 0, sectionEnd)
	if err != nil {
		t.Fatalf("BuildSectionBloomSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	blockRaw, err := block.Marshal()
	if err != nil {
		t.Fatalf("Marshal block: %v", err)
	}
	if err := rawdb.DeleteFrozenBlockRange(hot, sectionEnd, sectionEnd); err != nil {
		t.Fatalf("DeleteFrozenBlockRange: %v", err)
	}
	chainDB := rawdb.NewChainDB(hot, sectionBloomPruneAncientBlock(sectionEnd, blockRaw))

	result, err := PruneHotSectionBloomsWithProgress(chainDB, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotSectionBloomsWithProgress: %v", err)
	}
	if !result.HasRange || result.RowsDeleted != 1 {
		t.Fatalf("prune result = %+v, want one deleted section bloom row", result)
	}
	stage, ok, err := rawdb.ReadStageProgressRow(chainDB, rawdb.StageSnapshotSectionBloomPrune)
	if err != nil || !ok || !stage.HasBlockHash || stage.BlockHash != block.Hash() {
		t.Fatalf("section bloom prune stage row = %+v ok=%v err=%v, want ancient block hash %x", stage, ok, err, block.Hash())
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
	block := canonicalBoundaryTestBlock(t, rawdb.SectionBloomBlockPerSection-1)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock section end: %v", err)
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
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotSectionBloomPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("section bloom prune stage row = %+v ok=%v err=%v, want block %d hash %x", row, ok, err, block.Number(), block.Hash())
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

type sectionBloomPruneTestAncient struct {
	kind   string
	number uint64
	raw    []byte
}

func sectionBloomPruneAncientBlock(number uint64, raw []byte) *sectionBloomPruneTestAncient {
	return &sectionBloomPruneTestAncient{
		kind:   rawdb.AncientBlocksTable,
		number: number,
		raw:    append([]byte(nil), raw...),
	}
}

func (a *sectionBloomPruneTestAncient) Ancient(kind string, number uint64) ([]byte, error) {
	if a == nil || kind != a.kind || number != a.number {
		return nil, rawdb.ErrNotInAncient
	}
	return append([]byte(nil), a.raw...), nil
}

func (a *sectionBloomPruneTestAncient) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}
	if a == nil || kind != a.kind || start > a.number || a.number-start >= count {
		return nil, rawdb.ErrNotInAncient
	}
	raw := append([]byte(nil), a.raw...)
	if maxBytes > 0 && uint64(len(raw)) > maxBytes {
		return nil, rawdb.ErrNotInAncient
	}
	return [][]byte{raw}, nil
}

func (a *sectionBloomPruneTestAncient) AncientCount(kind string) (uint64, error) {
	if a == nil || kind != a.kind {
		return 0, nil
	}
	return a.number + 1, nil
}

func (a *sectionBloomPruneTestAncient) HasAncient(kind string, number uint64) (bool, error) {
	return a != nil && kind == a.kind && number == a.number, nil
}
