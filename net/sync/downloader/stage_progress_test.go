package downloader

import (
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
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

func TestStageProgressCollectorWritesPlannedBoundaryPrefix(t *testing.T) {
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 1, tcommon.Hash{0x01})
	collector.Observe(rawdb.StageBodies, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageExecution, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageCommitment, 3, tcommon.Hash{0x03})
	collector.Observe(rawdb.StageFinish, 2, tcommon.Hash{0x02})
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
		{Stage: rawdb.StageSyncImport, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("written progress = %+v, want %+v", got, want)
	}
	if rows := collector.Rows(2); !reflect.DeepEqual(rows, want) {
		t.Fatalf("progress rows = %+v, want %+v", rows, want)
	}
}

func TestStageProgressCollectorDoesNotPublishWithoutImportBoundary(t *testing.T) {
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 1, tcommon.Hash{0x01})
	collector.Observe(rawdb.StageExecution, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageCommitment, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageFinish, 2, tcommon.Hash{0x02})

	if rows := collector.Rows(2); len(rows) != 0 {
		t.Fatalf("rows without import boundary = %+v, want none", rows)
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
	})
	if called {
		t.Fatal("zero-value collector wrote downstream progress without import boundary")
	}

	zero.Observe(rawdb.StageBodies, 7, tcommon.Hash{0x07})
	zero.Observe(rawdb.StageExecution, 7, tcommon.Hash{0x07})
	zero.Write(7, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		called = true
		if stage != rawdb.StageSyncImport && stage != rawdb.StageSyncExecution {
			t.Fatalf("zero-value collector wrote unexpected stage %s/%d/%x", stage, blockNum, blockHash)
		}
	})
	if !called {
		t.Fatal("zero-value collector did not write planned boundary progress")
	}
}

func TestPlanImportedBatchProgress(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2, block3},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
			{Num: block3.Number(), Hash: block3.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	for _, stage := range []rawdb.StageID{rawdb.StageBodies, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageFinish} {
		collector.Observe(stage, block1.Number(), block1.Hash())
		collector.Observe(stage, block2.Number(), block2.Hash())
		collector.Observe(stage, block3.Number(), block3.Hash())
	}

	got := PlanImportedBatchProgress(batch, 2, collector)
	if !got.OK || got.Summary.Applied != 2 || got.Summary.Last.Num != block2.Number() {
		t.Fatalf("plan summary = %+v, want applied through block2", got.Summary)
	}
	wantStages := ImportPipelineStageTasks(block2.Number(), block2.Hash())
	if !reflect.DeepEqual(got.Stages, wantStages) {
		t.Fatalf("stages = %+v, want %+v", got.Stages, wantStages)
	}
	if len(got.Deletes) != 2 || got.Deletes[0].Number != block1.Number() || got.Deletes[1].Number != block2.Number() {
		t.Fatalf("deletes = %+v, want block1/block2 staged rows", got.Deletes)
	}
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncCommitment, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncFinish, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want %+v", got.Progress, wantRows)
	}
	if len(got.Decisions) != len(wantRows) {
		t.Fatalf("decisions = %+v, want one per sync stage", got.Decisions)
	}
	for _, decision := range got.Decisions {
		if decision.Status != ImportStageProgressPlanned {
			t.Fatalf("decision = %+v, want planned", decision)
		}
	}
}

func TestPlanImportedBatchProgressStopsAtStageMismatch(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageCommitment, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	got := PlanImportedBatchProgress(batch, 2, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want only import row before execution mismatch", got.Progress)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressMismatch,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
	if got.Decisions[1].Row.BlockNum != block1.Number() || got.Decisions[1].Row.BlockHash != block1.Hash() {
		t.Fatalf("mismatch row = %+v, want execution at block1", got.Decisions[1].Row)
	}
}

func TestPlanImportedBatchProgressStopsAtStageGap(t *testing.T) {
	block := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block},
		Buffered: []BufferedBlock{
			{Num: block.Number(), Hash: block.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block.Number(), block.Hash())
	collector.Observe(rawdb.StageCommitment, block.Number(), block.Hash())
	collector.Observe(rawdb.StageFinish, block.Number(), block.Hash())

	got := PlanImportedBatchProgress(batch, 1, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want only import row before execution gap", got.Progress)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressMissing,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
}

func TestImportPipelineStageTasks(t *testing.T) {
	hash := tcommon.Hash{0x42}
	got := ImportPipelineStageTasks(7, hash)
	want := []ImportStageTask{
		{CanonicalStage: rawdb.StageBodies, SyncStage: rawdb.StageSyncImport, BlockNum: 7, BlockHash: hash},
		{CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, BlockNum: 7, BlockHash: hash},
		{CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, BlockNum: 7, BlockHash: hash},
		{CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, BlockNum: 7, BlockHash: hash},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ImportPipelineStageTasks = %+v, want %+v", got, want)
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

func TestRepairSyncPipelineProgressDeletesDownstreamAheadOfUpstream(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	canonical := map[uint64]tcommon.Hash{
		1: {0x01},
		2: {0x02},
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, 1, canonical[1]); err != nil {
		t.Fatalf("write import progress: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if err := rawdb.WriteStageProgressWithHash(db, stage, 2, canonical[2]); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	got := RepairSyncPipelineProgress(db, 2, func(number uint64) (tcommon.Hash, bool) {
		hash, ok := canonical[number]
		return hash, ok
	})
	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressKept,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
	}
	if len(got) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d", got, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, got[i], status)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != 1 {
		t.Fatalf("import progress = %+v ok=%v err=%v, want block1 kept", row, ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressDeletesDownstreamWithoutUpstream(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x01}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, 1, hash); err != nil {
		t.Fatalf("write execution progress: %v", err)
	}

	got := RepairSyncPipelineProgress(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return hash, true
		}
		return tcommon.Hash{}, false
	})
	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressMissing,
		SyncStageProgressDeleted,
		SyncStageProgressMissing,
		SyncStageProgressMissing,
	}
	if len(got) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d", got, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, got[i], status)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncExecution); err != nil || ok {
		t.Fatalf("execution progress = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}
