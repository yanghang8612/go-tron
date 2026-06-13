package downloader

import (
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestStageForCanonicalStage(t *testing.T) {
	tests := []struct {
		canonical rawdb.StageID
		sync      rawdb.StageID
		ok        bool
	}{
		{rawdb.StageHeaders, "", false},
		{rawdb.StageBodies, rawdb.StageSyncImport, true},
		{rawdb.StageExecution, rawdb.StageSyncExecution, true},
		{rawdb.StageCommitment, rawdb.StageSyncCommitment, true},
		{rawdb.StageFinish, rawdb.StageSyncFinish, true},
		{rawdb.StageSnapshotBuild, "", false},
	}
	for _, tt := range tests {
		got, ok := StageForCanonicalStage(tt.canonical)
		if got != tt.sync || ok != tt.ok {
			t.Fatalf("StageForCanonicalStage(%s) = %s/%v, want %s/%v", tt.canonical, got, ok, tt.sync, tt.ok)
		}
	}
}

func TestStageProgressCollectorWritesLatestRowsAtOrBelowBoundary(t *testing.T) {
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 1, tcommon.Hash{0x01})
	collector.Observe(rawdb.StageBodies, 3, tcommon.Hash{0x03})
	collector.Observe(rawdb.StageExecution, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageCommitment, 4, tcommon.Hash{0x04})
	collector.Observe(rawdb.StageFinish, 2, tcommon.Hash{0x22})
	collector.Observe(rawdb.StageHeaders, 2, tcommon.Hash{0xff})

	var got []rawdb.StageProgress
	collector.Write(2, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		got = append(got, rawdb.StageProgress{
			Stage:        stage,
			BlockNum:     blockNum,
			BlockHash:    blockHash,
			HasBlockHash: true,
		})
	})

	want := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: 1, BlockHash: tcommon.Hash{0x01}, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
		{Stage: rawdb.StageSyncFinish, BlockNum: 2, BlockHash: tcommon.Hash{0x22}, HasBlockHash: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("written progress = %+v, want %+v", got, want)
	}
	if rows := collector.Rows(2); !reflect.DeepEqual(rows, want) {
		t.Fatalf("progress rows = %+v, want %+v", rows, want)
	}
}

func TestStageProgressCollectorNilAndEmpty(t *testing.T) {
	var nilCollector *StageProgressCollector
	called := false
	nilCollector.Write(10, func(rawdb.StageID, uint64, tcommon.Hash) {
		called = true
	})
	if called {
		t.Fatal("nil collector wrote progress")
	}

	NewStageProgressCollector().Write(10, func(rawdb.StageID, uint64, tcommon.Hash) {
		called = true
	})
	if called {
		t.Fatal("empty collector wrote progress")
	}

	var zero StageProgressCollector
	zero.Observe(rawdb.StageFinish, 7, tcommon.Hash{0x07})
	zero.Write(7, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		called = true
		if stage != rawdb.StageSyncFinish || blockNum != 7 || blockHash != (tcommon.Hash{0x07}) {
			t.Fatalf("zero-value collector wrote %s/%d/%x", stage, blockNum, blockHash)
		}
	})
	if !called {
		t.Fatal("zero-value collector did not write observed progress")
	}
}

func TestSyncPipelineProgressStagesOrder(t *testing.T) {
	got := SyncPipelineProgressStages()
	want := []rawdb.StageID{
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
		rawdb.StageSyncFinish,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SyncPipelineProgressStages = %v, want %v", got, want)
	}
}

func TestRepairSyncStageProgress(t *testing.T) {
	stage := rawdb.StageSyncImport
	canonical := map[uint64]tcommon.Hash{
		2: {0x02},
		3: {0x03},
	}
	readCanonical := func(number uint64) (tcommon.Hash, bool) {
		hash, ok := canonical[number]
		return hash, ok
	}
	tests := []struct {
		name   string
		row    *rawdb.StageProgress
		head   uint64
		status SyncStageProgressRepairStatus
		kept   bool
	}{
		{name: "missing", head: 3, status: SyncStageProgressMissing},
		{name: "valid", row: &rawdb.StageProgress{Stage: stage, BlockNum: 2, BlockHash: canonical[2], HasBlockHash: true}, head: 3, status: SyncStageProgressKept, kept: true},
		{name: "unbound", row: &rawdb.StageProgress{Stage: stage, BlockNum: 2}, head: 3, status: SyncStageProgressDeleted},
		{name: "ahead", row: &rawdb.StageProgress{Stage: stage, BlockNum: 4, BlockHash: tcommon.Hash{0x04}, HasBlockHash: true}, head: 3, status: SyncStageProgressDeleted},
		{name: "fork hash", row: &rawdb.StageProgress{Stage: stage, BlockNum: 2, BlockHash: tcommon.Hash{0xee}, HasBlockHash: true}, head: 3, status: SyncStageProgressDeleted},
		{name: "missing canonical", row: &rawdb.StageProgress{Stage: stage, BlockNum: 1, BlockHash: tcommon.Hash{0x01}, HasBlockHash: true}, head: 3, status: SyncStageProgressDeleted},
	}
	for _, tt := range tests {
		db := rawdb.NewMemoryDatabase()
		if tt.row != nil {
			if tt.row.HasBlockHash {
				if err := rawdb.WriteStageProgressWithHash(db, stage, tt.row.BlockNum, tt.row.BlockHash); err != nil {
					t.Fatalf("%s: write progress: %v", tt.name, err)
				}
			} else if err := rawdb.WriteStageProgress(db, stage, tt.row.BlockNum); err != nil {
				t.Fatalf("%s: write progress: %v", tt.name, err)
			}
		}
		got := RepairSyncStageProgress(db, stage, tt.head, readCanonical)
		if got.Status != tt.status {
			t.Fatalf("%s: status = %v result %+v, want %v", tt.name, got.Status, got, tt.status)
		}
		_, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			t.Fatalf("%s: read progress after repair: %v", tt.name, err)
		}
		if ok != tt.kept {
			t.Fatalf("%s: kept row = %v, want %v", tt.name, ok, tt.kept)
		}
	}
}
