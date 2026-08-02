package rawdb

import (
	"bytes"
	"math"
	"testing"
)

func TestInspectDatabaseGroupsCurrentAndLegacyRows(t *testing.T) {
	db := NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })

	rows := []struct {
		key   []byte
		value []byte
	}{
		{txInfoKey(bytes.Repeat([]byte{0x11}, 32)), bytes.Repeat([]byte{0xaa}, 100)},
		{txInfoBlockKey(7), bytes.Repeat([]byte{0xbb}, 50)},
		{syncStagedBlockKey(9), bytes.Repeat([]byte{0xcc}, 25)},
		{append(append([]byte{}, stateAccountLatestPrefix...), bytes.Repeat([]byte{0x33}, 20)...), []byte("account")},
		{append([]byte("state-account-latest-v2-"), bytes.Repeat([]byte{0x44}, 20)...), []byte("legacy-account")},
		{historyPruneModeKey, []byte("full")},
		{incrMerkleLastTreeKey, []byte("tree")},
		{bytes.Repeat([]byte{0x22}, 32), []byte("legacy-trie")},
		{[]byte("mystery-key"), []byte("unknown")},
	}
	var wantKeyBytes, wantValueBytes uint64
	for _, row := range rows {
		if err := db.Put(row.key, row.value); err != nil {
			t.Fatalf("Put(%q): %v", row.key, err)
		}
		wantKeyBytes += uint64(len(row.key))
		wantValueBytes += uint64(len(row.value))
	}

	report, err := InspectDatabase(db, InspectOptions{})
	if err != nil {
		t.Fatalf("InspectDatabase: %v", err)
	}
	if report.Rows != uint64(len(rows)) {
		t.Fatalf("Rows = %d, want %d", report.Rows, len(rows))
	}
	if report.KeyBytes != wantKeyBytes || report.ValueBytes != wantValueBytes {
		t.Fatalf("bytes = keys %d values %d, want keys %d values %d", report.KeyBytes, report.ValueBytes, wantKeyBytes, wantValueBytes)
	}
	for _, name := range []string{
		"transaction-info",
		"transaction-info-by-block",
		"sync-staged-block",
		"state-account-latest",
		"legacy-state-account-latest-v2",
		"history-prune-mode",
		"incremental-merkle-last-tree",
		"legacy-trie-node",
		"unclassified",
	} {
		if stat := findInspectionKeyspace(t, report, name); stat.Rows != 1 {
			t.Errorf("%s rows = %d, want 1", name, stat.Rows)
		}
	}
	var percent float64
	for _, stat := range report.Keyspaces {
		percent += stat.Percent
	}
	if math.Abs(percent-100) > 0.000001 {
		t.Fatalf("percent sum = %.9f, want 100", percent)
	}
}

func TestInspectDatabaseRejectsNil(t *testing.T) {
	if _, err := InspectDatabase(nil, InspectOptions{}); err == nil {
		t.Fatal("InspectDatabase(nil) succeeded")
	}
}

func findInspectionKeyspace(t *testing.T, report DatabaseInspection, name string) KeyspaceStat {
	t.Helper()
	for _, stat := range report.Keyspaces {
		if stat.Name == name {
			return stat
		}
	}
	t.Fatalf("keyspace %q not found in %+v", name, report.Keyspaces)
	return KeyspaceStat{}
}
