package main

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestPersistedSyncRemainingBlocksRestoresRestartGate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if remaining, ok := persistedSyncRemainingBlocks(db, 100); ok || remaining != 0 {
		t.Fatalf("missing inventory target = %d/%v, want 0/false", remaining, ok)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, 1_000); err != nil {
		t.Fatalf("write sync inventory target: %v", err)
	}
	if remaining, ok := persistedSyncRemainingBlocks(db, 100); !ok || remaining != 900 {
		t.Fatalf("restored remaining = %d/%v, want 900/true", remaining, ok)
	}
	for _, head := range []uint64{1_000, 1_001} {
		if remaining, ok := persistedSyncRemainingBlocks(db, head); ok || remaining != 0 {
			t.Fatalf("completed target at head %d = %d/%v, want 0/false", head, remaining, ok)
		}
	}
}

func TestPersistedSyncRemainingBlocksRejectsImplausibleTarget(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, 10_000); err != nil {
		t.Fatalf("write sync inventory target: %v", err)
	}
	if remaining, ok := persistedSyncRemainingBlocksBounded(db, 100, 1_000); ok || remaining != 0 {
		t.Fatalf("implausible target = %d/%v, want 0/false", remaining, ok)
	}
	if remaining, ok := persistedSyncRemainingBlocksBounded(db, 100, 10_000); !ok || remaining != 9_900 {
		t.Fatalf("bounded valid target = %d/%v, want 9900/true", remaining, ok)
	}
}
