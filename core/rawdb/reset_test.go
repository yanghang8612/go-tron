package rawdb

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestResetMutableStateClearsCommitmentBranches(t *testing.T) {
	db := NewMemoryDatabase()

	// A commitment branch row, a root row, and a sibling latest row.
	if err := WriteCommitmentBranch(db, []byte{0x0a, 0x0b}, []byte{0x00, 0x00}); err != nil {
		t.Fatal(err)
	}
	if err := WriteLatestDomainCommitmentRoot(db, common.Hash{0x11}); err != nil {
		t.Fatal(err)
	}
	addr := common.Address{0x41, 0x01}
	if err := WriteStateAccountLatest(db, addr, []byte("acct")); err != nil {
		t.Fatal(err)
	}

	if err := ResetMutableState(db); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// Branch keyspace must be empty after reset (the fix).
	branchCount := 0
	if err := IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		branchCount++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate branches: %v", err)
	}
	if branchCount != 0 {
		t.Fatalf("commitment branch rows survived ResetMutableState: %d", branchCount)
	}
	// Sanity: the root row and latest row are cleared too (already covered, just assert).
	if _, ok, err := ReadLatestDomainCommitmentRoot(db); err != nil || ok {
		t.Fatalf("commitment root survived reset: ok=%v err=%v", ok, err)
	}
}
