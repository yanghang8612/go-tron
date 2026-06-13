package downloader

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestNewDiagnosticsSortsPeerState(t *testing.T) {
	diag := NewDiagnostics(3, 4, 5, []PeerDiagnostics{
		{
			ID:             "peer-b",
			Inflight:       2,
			FetchListLen:   7,
			PendingLen:     1,
			RemainNum:      9,
			ChainRequested: true,
			Done:           false,
		},
		{
			ID:           "peer-a",
			Inflight:     0,
			FetchListLen: 1,
			PendingLen:   0,
			RemainNum:    0,
			Done:         true,
		},
	})

	if diag.BlockBufferLen != 3 || diag.RequestedLen != 4 || diag.RetryListLen != 5 {
		t.Fatalf("counts = %d/%d/%d, want 3/4/5", diag.BlockBufferLen, diag.RequestedLen, diag.RetryListLen)
	}
	want := "peer-a{inflight=0 fetchList=1 pending=0 remain=0 chainRequested=false done=true};" +
		"peer-b{inflight=2 fetchList=7 pending=1 remain=9 chainRequested=true done=false}"
	if diag.PeerState != want {
		t.Fatalf("PeerState = %q, want %q", diag.PeerState, want)
	}
}

func TestNewDiagnosticsOmitsEmptyPeerIDs(t *testing.T) {
	diag := NewDiagnostics(0, 0, 0, []PeerDiagnostics{
		{ID: "", Inflight: 99},
		{ID: "peer", Done: true},
	})
	want := "peer{inflight=0 fetchList=0 pending=0 remain=0 chainRequested=false done=true}"
	if diag.PeerState != want {
		t.Fatalf("PeerState = %q, want %q", diag.PeerState, want)
	}
}

func TestNewDiagnosticsWithoutPeersHasNoPeerState(t *testing.T) {
	diag := NewDiagnostics(1, 2, 3, nil)
	if diag.PeerState != "" {
		t.Fatalf("PeerState = %q, want empty", diag.PeerState)
	}
}

func TestDiagnosticsWithImportStagePlan(t *testing.T) {
	hash := tcommon.Hash{0x42}
	collector := NewStageProgressCollector()
	for _, stage := range []rawdb.StageID{rawdb.StageBodies, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageFinish} {
		collector.Observe(stage, 7, hash)
	}
	complete := collector.PlanSchedule(NewImportStageSchedule(7, hash))
	diag := NewDiagnostics(1, 2, 3, nil).WithImportStagePlan(complete)
	if !diag.ImportStageComplete || diag.ImportStageCompleted != 4 || diag.ImportStageScheduled != 4 {
		t.Fatalf("complete diagnostics = %+v, want complete 4/4", diag)
	}
	if diag.ImportStageNext != "" || diag.ImportStageBlockedStatus != "" {
		t.Fatalf("complete diagnostics has blocked stage: %+v", diag)
	}
	if !diag.HasImportStagePlan() {
		t.Fatal("complete diagnostics did not report stage plan")
	}

	collector = NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 8, hash)
	collector.Observe(rawdb.StageExecution, 8, hash)
	blocked := collector.PlanSchedule(NewImportStageSchedule(8, hash))
	diag = NewDiagnostics(0, 0, 0, nil).WithImportStagePlan(blocked)
	if diag.ImportStageComplete || diag.ImportStageCompleted != 2 || diag.ImportStageScheduled != 4 {
		t.Fatalf("blocked diagnostics = %+v, want incomplete 2/4", diag)
	}
	if diag.ImportStageNext != string(ImportStagePhaseCommitment) || diag.ImportStageBlockedStatus != ImportStageProgressMissing.String() {
		t.Fatalf("blocked stage diagnostics = %+v, want commitment/missing", diag)
	}
	if diag.ImportStageNextBlock != 8 || diag.ImportStageNextCanonical != string(rawdb.StageCommitment) || diag.ImportStageNextSync != string(rawdb.StageSyncCommitment) {
		t.Fatalf("blocked stage target = block %d canonical %q sync %q, want block8 commitment/sync-commitment",
			diag.ImportStageNextBlock, diag.ImportStageNextCanonical, diag.ImportStageNextSync)
	}

	diag = NewDiagnostics(0, 0, 0, nil).WithImportStageDiagnostics(blocked.Diagnostics())
	if diag.ImportStageNext != string(ImportStagePhaseCommitment) || diag.ImportStageNextBlock != 8 {
		t.Fatalf("direct diagnostics = %+v, want commitment at block8", diag)
	}
}

func TestDiagnosticsWithImportBatchExecutionPlan(t *testing.T) {
	hash1 := tcommon.Hash{0x01}
	hash2 := tcommon.Hash{0x02}
	stagePlan := NewImportBatchStagePlan([]ImportStageSchedule{
		NewImportStageSchedule(1, hash1),
		NewImportStageSchedule(2, hash2),
	})
	execution := NewImportBatchExecutionPlanDiagnostics(stagePlan.Schedules, stagePlan)

	diag := NewDiagnostics(0, 0, 0, nil).WithImportBatchExecutionDiagnostics(execution)
	if !diag.HasImportBatchExecutionPlan() {
		t.Fatal("diagnostics did not report import execution plan")
	}
	if diag.ImportExecutionPlannedBlocks != 2 || diag.ImportExecutionPlannedStages != 8 || diag.ImportExecutionPostBodyStages != 6 {
		t.Fatalf("execution diagnostics counts = %+v, want 2 blocks/8 stages/6 post-body", diag)
	}
	if diag.ImportExecutionFirstBlock != 1 || diag.ImportExecutionLastBlock != 2 {
		t.Fatalf("execution diagnostics range = %d..%d, want 1..2", diag.ImportExecutionFirstBlock, diag.ImportExecutionLastBlock)
	}
	if NewDiagnostics(0, 0, 0, nil).HasImportBatchExecutionPlan() {
		t.Fatal("empty diagnostics reported import execution plan")
	}
}
