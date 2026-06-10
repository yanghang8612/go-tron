package rawdb

import "testing"

func TestHistoryPruneModeAccessor(t *testing.T) {
	db := NewMemoryDatabase()

	if mode, ok, err := ReadHistoryPruneMode(db); err != nil || ok || mode != "" {
		t.Fatalf("missing prune mode: mode=%q ok=%v err=%v", mode, ok, err)
	}
	if err := WriteHistoryPruneMode(db, "archive"); err != nil {
		t.Fatalf("write prune mode: %v", err)
	}
	if mode, ok, err := ReadHistoryPruneMode(db); err != nil || !ok || mode != "archive" {
		t.Fatalf("read prune mode: mode=%q ok=%v err=%v", mode, ok, err)
	}
}
