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
	delta, err := NewCommitmentBranchDeltaKeyspace(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := delta.Write(db, []byte{0x0c}, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if err := WriteCommitmentBranchBase(db, CommitmentBranchBase{Generation: 3, SnapshotTxNum: 10, Root: common.Hash{0x11}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteCommitmentBranchRotation(db, CommitmentBranchRotation{Generation: 4, SnapshotTxNum: 11, Root: common.Hash{0x12}, BlockNum: 9, BlockHash: common.Hash{0x13}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteLatestDomainCommitmentRoot(db, common.Hash{0x11}); err != nil {
		t.Fatal(err)
	}
	addr := common.Address{0x41, 0x01}
	if err := WriteStateAccountLatest(db, addr, []byte("acct")); err != nil {
		t.Fatal(err)
	}
	if err := WriteHistoryPruneMode(db, "archive"); err != nil {
		t.Fatal(err)
	}
	hash := stateChangePostingHash([]byte("latest"))
	posting, err := encodeStateChangePosting([]uint64{9})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangePostingKey(hash, 9), posting); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangeKeyDirectoryKey([]byte("latest")), nil); err != nil {
		t.Fatal(err)
	}
	staged := testSyncStagedBlock(1, common.Hash{0x01})
	if err := WriteSyncStagedBlock(db, staged); err != nil {
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
	deltaCount := 0
	if err := delta.Iterate(db, func(_, _ []byte) (bool, error) {
		deltaCount++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if deltaCount != 0 {
		t.Fatalf("commitment delta rows survived ResetMutableState: %d", deltaCount)
	}
	if _, ok, err := ReadCommitmentBranchBase(db); err != nil || ok {
		t.Fatalf("commitment base survived reset: ok=%v err=%v", ok, err)
	}
	if _, ok, err := ReadCommitmentBranchRotation(db); err != nil || ok {
		t.Fatalf("commitment rotation survived reset: ok=%v err=%v", ok, err)
	}
	// Sanity: the root row and latest row are cleared too (already covered, just assert).
	if _, ok, err := ReadLatestDomainCommitmentRoot(db); err != nil || ok {
		t.Fatalf("commitment root survived reset: ok=%v err=%v", ok, err)
	}
	if mode, ok, err := ReadHistoryPruneMode(db); err != nil || !ok || mode != "archive" {
		t.Fatalf("history prune mode should survive reset: mode=%q ok=%v err=%v", mode, ok, err)
	}
	if _, ok, err := ReadSyncStagedBlock(db, staged.Number()); err != nil || ok {
		t.Fatalf("sync staged block survived reset: ok=%v err=%v", ok, err)
	}
	for _, prefix := range [][]byte{stateChangePostingPrefix, stateChangeKeyDirectoryPrefix} {
		it := db.NewIterator(prefix, nil)
		hasRow := it.Next()
		it.Release()
		if hasRow {
			t.Fatalf("state change posting prefix %q survived reset", prefix)
		}
	}
}
