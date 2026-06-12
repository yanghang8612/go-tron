package rawdb

import (
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestStageProgressReadWriteIterateDelete(t *testing.T) {
	db := NewMemoryDatabase()
	if _, ok, err := ReadStageProgress(db, StageExecution); err != nil || ok {
		t.Fatalf("empty stage progress ok=%v err=%v", ok, err)
	}
	if err := WriteStageProgress(db, StageExecution, 42); err != nil {
		t.Fatalf("write execution progress: %v", err)
	}
	if err := WriteStageProgress(db, StageCommitment, 41); err != nil {
		t.Fatalf("write commitment progress: %v", err)
	}
	executionHash := common.Hash{0x2a}
	if err := WriteStageProgressWithHash(db, StageExecution, 42, executionHash); err != nil {
		t.Fatalf("write hash-bound execution progress: %v", err)
	}
	if got, ok, err := ReadStageProgress(db, StageExecution); err != nil || !ok || got != 42 {
		t.Fatalf("read execution progress = %d ok=%v err=%v", got, ok, err)
	}
	if row, ok, err := ReadStageProgressRow(db, StageExecution); err != nil || !ok || row.BlockNum != 42 || !row.HasBlockHash || row.BlockHash != executionHash {
		t.Fatalf("read execution progress row = %+v ok=%v err=%v, want hash-bound 42", row, ok, err)
	}
	var got []StageProgress
	if err := IterateStageProgress(db, func(progress StageProgress) (bool, error) {
		got = append(got, progress)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate stage progress: %v", err)
	}
	if len(got) != 2 || got[0].Stage != StageCommitment || got[0].BlockNum != 41 || got[0].HasBlockHash ||
		got[1].Stage != StageExecution || got[1].BlockNum != 42 || !got[1].HasBlockHash || got[1].BlockHash != executionHash {
		t.Fatalf("stage progress rows = %+v", got)
	}
	if err := DeleteStageProgress(db, StageExecution); err != nil {
		t.Fatalf("delete execution progress: %v", err)
	}
	if _, ok, err := ReadStageProgress(db, StageExecution); err != nil || ok {
		t.Fatalf("deleted stage progress ok=%v err=%v", ok, err)
	}
}

func TestReadVerifiedStageProgressBlock(t *testing.T) {
	db := NewMemoryDatabase()
	block := testSyncStagedBlock(3, common.Hash{0x02})
	if err := WriteBlock(db, block); err != nil {
		t.Fatalf("write block: %v", err)
	}
	if err := WriteStageProgressWithHash(db, StageFinish, block.Number(), block.Hash()); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	got, ok, err := ReadVerifiedStageProgressBlock(db, StageFinish)
	if err != nil || !ok || got != block.Number() {
		t.Fatalf("verified finish stage = %d ok=%v err=%v, want block %d", got, ok, err, block.Number())
	}

	if err := WriteStageProgress(db, StageFinish, block.Number()); err != nil {
		t.Fatalf("write unbound finish stage: %v", err)
	}
	if _, ok, err := ReadVerifiedStageProgressBlock(db, StageFinish); err == nil || !ok || !strings.Contains(err.Error(), "finish stage 3 is not hash-bound") {
		t.Fatalf("unbound verified finish stage ok=%v err=%v, want not hash-bound", ok, err)
	}

	if err := WriteStageProgressWithHash(db, StageFinish, block.Number(), common.Hash{0xee}); err != nil {
		t.Fatalf("write mismatched finish stage: %v", err)
	}
	if _, ok, err := ReadVerifiedStageProgressBlock(db, StageFinish); err == nil || !ok || !strings.Contains(err.Error(), "finish stage 3 hash") {
		t.Fatalf("mismatched verified finish stage ok=%v err=%v, want hash mismatch", ok, err)
	}

	if err := WriteStageProgressWithHash(db, StageCommitment, 9, common.Hash{0x09}); err != nil {
		t.Fatalf("write missing-block commitment stage: %v", err)
	}
	if _, ok, err := ReadVerifiedStageProgressBlock(db, StageCommitment); err == nil || !ok || !strings.Contains(err.Error(), "commitment stage 9 has hash") {
		t.Fatalf("missing-block verified commitment stage ok=%v err=%v, want unavailable block", ok, err)
	}

	if _, ok, err := ReadVerifiedStageProgressBlock(db, StageExecution); err != nil || ok {
		t.Fatalf("missing verified execution stage ok=%v err=%v, want absent", ok, err)
	}
}

func TestReadVerifiedStageProgressBlockWithHashReader(t *testing.T) {
	db := NewMemoryDatabase()
	hash := common.Hash{0x42}
	if err := WriteStageProgressWithHash(db, StageFinish, 99, hash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	got, ok, err := ReadVerifiedStageProgressBlockWithHashReader(db, StageFinish, func(number uint64) common.Hash {
		if number != 99 {
			return common.Hash{}
		}
		return hash
	})
	if err != nil || !ok || got != 99 {
		t.Fatalf("custom-hash verified finish stage = %d ok=%v err=%v, want block 99", got, ok, err)
	}
}

func TestRestoreSyncInventoryTarget(t *testing.T) {
	tests := []struct {
		name     string
		head     uint64
		row      uint64
		haveRow  bool
		target   uint64
		restored bool
	}{
		{name: "missing", head: 10, target: 10},
		{name: "ahead", head: 10, row: 25, haveRow: true, target: 25, restored: true},
		{name: "stale", head: 10, row: 7, haveRow: true, target: 10},
		{name: "equal", head: 10, row: 10, haveRow: true, target: 10},
	}
	for _, tt := range tests {
		db := NewMemoryDatabase()
		if tt.haveRow {
			if err := WriteStageProgress(db, StageSyncInventory, tt.row); err != nil {
				t.Fatalf("%s: write sync inventory: %v", tt.name, err)
			}
		}
		got := RestoreSyncInventoryTarget(db, tt.head)
		if got.Target != tt.target || got.Restored != tt.restored || got.ReadError != nil || got.HaveRow != tt.haveRow {
			t.Fatalf("%s: restore = %+v, want target %d restored %v haveRow %v", tt.name, got, tt.target, tt.restored, tt.haveRow)
		}
	}
}

func TestCanonicalStageProgressWriteAndRewind(t *testing.T) {
	db := NewMemoryDatabase()
	hash12 := common.Hash{0x12}
	if err := WriteCanonicalStageProgressWithHash(db, 12, hash12); err != nil {
		t.Fatalf("write canonical progress: %v", err)
	}
	for _, stage := range CanonicalExecutionStages() {
		if row, ok, err := ReadStageProgressRow(db, stage); err != nil || !ok || row.BlockNum != 12 || !row.HasBlockHash || row.BlockHash != hash12 {
			t.Fatalf("%s progress after write = %+v ok=%v err=%v, want 12 hash", stage, row, ok, err)
		}
	}
	for _, stage := range []StageID{StageChainFreezer, StageSyncInventory, StageSyncBodies, StageSyncBodiesReady, StageSyncImport, StageSyncExecution, StageSyncCommitment, StageSyncFinish, StageSnapshotEventLogBuild, StageSnapshotChainFreezerTailPrune} {
		if _, ok, err := ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s downloader progress should not be written by canonical helper: ok=%v err=%v", stage, ok, err)
		}
	}
	hash7 := common.Hash{0x07}
	if err := RewindCanonicalStageProgressWithHash(db, 7, hash7); err != nil {
		t.Fatalf("rewind canonical progress: %v", err)
	}
	for _, stage := range CanonicalExecutionStages() {
		if row, ok, err := ReadStageProgressRow(db, stage); err != nil || !ok || row.BlockNum != 7 || !row.HasBlockHash || row.BlockHash != hash7 {
			t.Fatalf("%s progress after rewind = %+v ok=%v err=%v, want 7 hash", stage, row, ok, err)
		}
	}
}
