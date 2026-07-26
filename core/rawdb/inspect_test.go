package rawdb

import (
	"bytes"
	"math"
	"testing"
)

func TestInspectDatabaseGroupsAndAccountsAllRows(t *testing.T) {
	db := NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })

	hash := bytes.Repeat([]byte{0x11}, 32)
	rows := []struct {
		key   []byte
		value []byte
	}{
		{txInfoKey(hash), bytes.Repeat([]byte{0xaa}, 100)},
		{txInfoBlockKey(7), bytes.Repeat([]byte{0xbb}, 50)},
		{incrMerkleLastTreeKey, []byte("tree")},
		{bytes.Repeat([]byte{0x22}, 32), []byte("legacy")},
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
	if report.LogicalBytes != wantKeyBytes+wantValueBytes {
		t.Fatalf("LogicalBytes = %d, want %d", report.LogicalBytes, wantKeyBytes+wantValueBytes)
	}

	for _, name := range []string{
		"transaction-info",
		"transaction-info-by-block",
		"incremental-merkle-last-tree",
		"legacy-trie-node",
		"unclassified",
	} {
		stat := findKeyspaceStat(t, report, name)
		if stat.Rows != 1 {
			t.Errorf("%s rows = %d, want 1", name, stat.Rows)
		}
	}
	if got := findKeyspaceStat(t, report, "incremental-merkle-last-tree").KeyPattern; got != "=imt-LAST_TREE" {
		t.Fatalf("merkle sentinel pattern = %q", got)
	}
	if report.Keyspaces[0].Name != "transaction-info" {
		t.Fatalf("largest keyspace = %q, want transaction-info", report.Keyspaces[0].Name)
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

func findKeyspaceStat(t *testing.T, report DatabaseInspection, name string) KeyspaceStat {
	t.Helper()
	for _, stat := range report.Keyspaces {
		if stat.Name == name {
			return stat
		}
	}
	t.Fatalf("keyspace %q not found in %+v", name, report.Keyspaces)
	return KeyspaceStat{}
}
