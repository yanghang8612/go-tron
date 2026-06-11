package snapshots

import (
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPruneHotBalanceTracesKeepsColdReads(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	ownerA := balanceTraceTestAddress(0xd1)
	ownerB := balanceTraceTestAddress(0xd2)

	if err := rawdb.WriteBlockBalanceTrace(db, 10, balanceTraceTestBlockTrace(10, 1000)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 10: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 12: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, ownerA.Bytes(), 10, 100); err != nil {
		t.Fatalf("WriteAccountTrace ownerA: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, ownerB.Bytes(), 12, 200); err != nil {
		t.Fatalf("WriteAccountTrace ownerB: %v", err)
	}

	ref, err := BuildBalanceTraceSegmentFromDB(db, snapshotDir, "", 10, 20)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	result, err := PruneHotBalanceTraces(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("PruneHotBalanceTraces: %v", err)
	}
	if !result.HasRange || result.FromBlock != 10 || result.ToBlock != 12 ||
		result.ColdTraceSegments != 1 || result.BlockTracesDeleted != 2 || result.AccountTracesDeleted != 2 {
		t.Fatalf("prune result = %+v, want range 10..12 and two block/account rows", result)
	}
	if got := rawdb.ReadBlockBalanceTrace(db, 12); got != nil {
		t.Fatalf("hot ReadBlockBalanceTrace after prune = %+v, want nil", got)
	}
	if balance, ok := rawdb.ReadAccountTrace(db, ownerB.Bytes(), 12); ok || balance != 0 {
		t.Fatalf("hot ReadAccountTrace after prune = %d/%v, want 0/false", balance, ok)
	}

	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	db.SetBalanceTraceReader(mgr)
	if got := rawdb.ReadBlockBalanceTrace(db, 12); got == nil || got.GetTimestamp() != 1200 {
		t.Fatalf("cold ReadBlockBalanceTrace after prune = %+v, want timestamp 1200", got)
	}
	block, balance, ok, err := rawdb.ReadAccountTraceAtOrBefore(db, ownerB.Bytes(), 20)
	if err != nil || !ok || block != 12 || balance != 200 {
		t.Fatalf("cold ReadAccountTraceAtOrBefore after prune = %d/%d/%v/%v, want 12/200/true/nil", block, balance, ok, err)
	}
}

func TestPruneHotBalanceTracesRejectsColdMismatchBeforeDeleting(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	owner := balanceTraceTestAddress(0xe1)
	if err := rawdb.WriteBlockBalanceTrace(db, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace original: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, owner.Bytes(), 12, 120); err != nil {
		t.Fatalf("WriteAccountTrace original: %v", err)
	}
	ref, err := BuildBalanceTraceSegmentFromDB(db, snapshotDir, "", 10, 20)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 12, balanceTraceTestBlockTrace(12, 9999)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace changed: %v", err)
	}

	_, err = PruneHotBalanceTraces(db, snapshotDir, manifest)
	if err == nil || !strings.Contains(err.Error(), "differs from hot block trace") {
		t.Fatalf("PruneHotBalanceTraces error = %v, want cold/hot mismatch", err)
	}
	if got := rawdb.ReadBlockBalanceTrace(db, 12); got == nil || got.GetTimestamp() != 9999 {
		t.Fatalf("hot block trace after failed prune = %+v, want changed timestamp 9999", got)
	}
	if balance, ok := rawdb.ReadAccountTrace(db, owner.Bytes(), 12); !ok || balance != 120 {
		t.Fatalf("hot account trace after failed prune = %d/%v, want 120/true", balance, ok)
	}
}

func TestPruneHotBalanceTracesWithProgressSkipsProcessedBlocks(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	ownerA := balanceTraceTestAddress(0xf1)
	ownerB := balanceTraceTestAddress(0xf2)
	if err := rawdb.WriteBlockBalanceTrace(db, 10, balanceTraceTestBlockTrace(10, 1000)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 10: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace 12: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, ownerA.Bytes(), 10, 100); err != nil {
		t.Fatalf("WriteAccountTrace ownerA: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, ownerB.Bytes(), 12, 200); err != nil {
		t.Fatalf("WriteAccountTrace ownerB: %v", err)
	}
	refA, err := BuildBalanceTraceSegmentFromDB(db, snapshotDir, "", 10, 10)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB A: %v", err)
	}
	refB, err := BuildBalanceTraceSegmentFromDB(db, snapshotDir, "", 11, 20)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB B: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{refA, refB})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	first, err := PruneHotBalanceTracesWithProgress(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("first PruneHotBalanceTracesWithProgress: %v", err)
	}
	if !first.HasRange || first.FromBlock != 10 || first.ToBlock != 12 ||
		first.ColdTraceSegments != 2 || first.BlockTracesDeleted != 2 || first.AccountTracesDeleted != 2 {
		t.Fatalf("first prune result = %+v, want range 10..12 with two block/account rows", first)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBalanceTracePrune); err != nil || !ok || got != 20 {
		t.Fatalf("balance trace prune stage = %d ok=%v err=%v, want 20", got, ok, err)
	}

	second, err := PruneHotBalanceTracesWithProgress(db, snapshotDir, manifest)
	if err != nil {
		t.Fatalf("second PruneHotBalanceTracesWithProgress: %v", err)
	}
	if second.HasRange || second.ColdTraceSegments != 0 || second.BlockTracesDeleted != 0 || second.AccountTracesDeleted != 0 {
		t.Fatalf("second prune result = %+v, want no repeated work", second)
	}
}

func TestBalanceTracePruneLifecycleOnePass(t *testing.T) {
	root := t.TempDir()
	snapshotDir := root + "/snapshot"
	db := rawdb.NewMemoryChainDB()
	owner := balanceTraceTestAddress(0xf3)
	if err := rawdb.WriteBlockBalanceTrace(db, 12, balanceTraceTestBlockTrace(12, 1200)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, owner.Bytes(), 12, 120); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}
	ref, err := BuildBalanceTraceSegmentFromDB(db, snapshotDir, "", 12, 12)
	if err != nil {
		t.Fatalf("BuildBalanceTraceSegmentFromDB: %v", err)
	}
	manifest := NewManifest(0, 0, []SegmentRef{ref})
	if err := PublishManifest(snapshotDir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	lifecycle := NewBalanceTracePruneLifecycle(db, BalanceTracePruneLifecycleConfig{Dir: snapshotDir})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result == nil || !result.HasRange || result.BlockTracesDeleted != 1 || result.AccountTracesDeleted != 1 {
		t.Fatalf("OnePass result = %+v, want one block/account row", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBalanceTracePrune); err != nil || !ok || got != 12 {
		t.Fatalf("balance trace prune stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
}

func TestBalanceTracePruneLifecycleNoManifestNoop(t *testing.T) {
	lifecycle := NewBalanceTracePruneLifecycle(rawdb.NewMemoryChainDB(), BalanceTracePruneLifecycleConfig{Dir: t.TempDir()})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if result != nil {
		t.Fatalf("OnePass result = %+v, want nil without manifest", result)
	}
}
