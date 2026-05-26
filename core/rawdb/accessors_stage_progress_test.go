package rawdb

import (
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
