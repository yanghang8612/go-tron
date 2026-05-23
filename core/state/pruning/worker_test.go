package pruning

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestWorkerPrunesDomainHistoryAndCheckpoints(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x33}, common.AccountIDLength)...))
	hash1 := common.Hash{0x01}
	hash4 := common.Hash{0x04}
	key := []byte("k")

	for _, blockNum := range []uint64{1, 4} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum:   blockNum,
			BlockHash:  common.Hash{byte(blockNum)},
			TxNum:      blockNum,
			Seq:        1,
			Owner:      owner,
			Generation: 0,
			Domain:     kvdomains.SystemDynamicProperty,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("prev"),
			NextExists: true,
			Next:       []byte("next"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{BlockNum: 1, BlockHash: hash1, Root: hash1, Scheme: rawdb.LatestDomainCommitmentScheme}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{BlockNum: 4, BlockHash: hash4, Root: hash4, Scheme: rawdb.LatestDomainCommitmentScheme}); err != nil {
		t.Fatal(err)
	}

	stats, err := Run(db, FullPolicy(3, 2), 5)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if stats.DeletedTxRanges != 1 || stats.DeletedDomainChangeBlocks != 1 || stats.DeletedCommitmentCheckpoints != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || ok {
		t.Fatalf("block 1 range survived ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 4); err != nil || !ok {
		t.Fatalf("block 4 range missing ok:%v err:%v", ok, err)
	}
	var touched []uint64
	if err := rawdb.IterateStateDomainChangeBlocks(db, owner, 0, kvdomains.SystemDynamicProperty, key, func(blockNum uint64) (bool, error) {
		touched = append(touched, blockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(touched) != 1 || touched[0] != 4 {
		t.Fatalf("inverse blocks = %v, want [4]", touched)
	}
	if _, ok, err := rawdb.ReadStateCommitmentCheckpoint(db, 1); err != nil || ok {
		t.Fatalf("block 1 checkpoint survived ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateCommitmentCheckpoint(db, 4); err != nil || !ok {
		t.Fatalf("block 4 checkpoint missing ok:%v err:%v", ok, err)
	}
	report, err := Check(db, FullPolicy(3, 2), 5, "")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(report.Warnings) != 0 || report.RetainedTxRanges != 1 || report.RetainedDomainChanges != 1 || report.CommitmentCheckpoints != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestCheckerValidatesSnapshotSegmentsAndCodeHashes(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x44}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	ref, err := snapshots.BuildLatestDomainSegmentFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 1, "latest/system-dp.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 1, []snapshots.SegmentRef{ref})); err != nil {
		t.Fatal(err)
	}
	report, err := Check(db, ArchivePolicy(), 1, dir)
	if err != nil {
		t.Fatalf("check snapshots: %v", err)
	}
	if report.SnapshotSegments != 1 || report.LatestRows != 1 {
		t.Fatalf("report = %+v", report)
	}
	code := []byte{0xde, 0xad}
	hash := common.Keccak256(code)
	if err := CheckCodeHashes(db, []common.Hash{hash}); err == nil {
		t.Fatal("missing code hash accepted")
	}
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if err := CheckCodeHashes(db, []common.Hash{hash}); err != nil {
		t.Fatalf("code hash check: %v", err)
	}
}
