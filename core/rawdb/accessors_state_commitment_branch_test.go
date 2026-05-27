package rawdb

import (
	"bytes"
	"testing"
)

func TestCommitmentBranchRoundTrip(t *testing.T) {
	db := NewMemoryDatabase()

	prefixes := [][]byte{
		{0x0A},
		{0x0A, 0x0B},
		{0xFF, 0x01, 0x02},
	}
	values := [][]byte{
		{0x01, 0x02, 0x03},
		{0xAA, 0xBB},
		{0xCC},
	}

	// Write 3 branches.
	for i, pfx := range prefixes {
		if err := WriteCommitmentBranch(db, pfx, values[i]); err != nil {
			t.Fatalf("WriteCommitmentBranch[%d]: %v", i, err)
		}
	}

	// Read each back and confirm equal.
	for i, pfx := range prefixes {
		got, ok, err := ReadCommitmentBranch(db, pfx)
		if err != nil {
			t.Fatalf("ReadCommitmentBranch[%d]: %v", i, err)
		}
		if !ok {
			t.Fatalf("ReadCommitmentBranch[%d]: not found", i)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("ReadCommitmentBranch[%d]: got %x want %x", i, got, values[i])
		}
	}

	// Iterate and collect all 3.
	collected := make(map[string][]byte)
	if err := IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		collected[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches: %v", err)
	}
	if len(collected) != 3 {
		t.Fatalf("iterate: got %d entries, want 3", len(collected))
	}
	for i, pfx := range prefixes {
		got, ok := collected[string(pfx)]
		if !ok {
			t.Fatalf("iterate: prefix[%d] not found", i)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("iterate: prefix[%d]: got %x want %x", i, got, values[i])
		}
	}

	// Delete one and confirm gone.
	if err := DeleteCommitmentBranch(db, prefixes[1]); err != nil {
		t.Fatalf("DeleteCommitmentBranch: %v", err)
	}
	_, ok, err := ReadCommitmentBranch(db, prefixes[1])
	if err != nil {
		t.Fatalf("read deleted: %v", err)
	}
	if ok {
		t.Fatal("read deleted: expected not found")
	}

	// Engine state row.
	engineData := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := WriteCommitmentEngineState(db, engineData); err != nil {
		t.Fatalf("WriteCommitmentEngineState: %v", err)
	}
	gotEngine, ok, err := ReadCommitmentEngineState(db)
	if err != nil {
		t.Fatalf("ReadCommitmentEngineState: %v", err)
	}
	if !ok {
		t.Fatal("ReadCommitmentEngineState: not found")
	}
	if !bytes.Equal(gotEngine, engineData) {
		t.Fatalf("ReadCommitmentEngineState: got %x want %x", gotEngine, engineData)
	}
}

func TestCommitmentBranchMissing(t *testing.T) {
	db := NewMemoryDatabase()
	_, ok, err := ReadCommitmentBranch(db, []byte{0x01})
	if err != nil {
		t.Fatalf("missing read: %v", err)
	}
	if ok {
		t.Fatal("missing read: expected not found")
	}

	_, ok, err = ReadCommitmentEngineState(db)
	if err != nil {
		t.Fatalf("missing engine state read: %v", err)
	}
	if ok {
		t.Fatal("missing engine state read: expected not found")
	}
}
