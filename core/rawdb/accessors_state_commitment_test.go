package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestStateCommitmentCheckpointRoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	cp := &StateCommitmentCheckpoint{
		BlockNum:  12,
		BlockHash: common.Hash{0x12},
		Root:      common.Hash{0xaa},
		Scheme:    LatestDomainCommitmentScheme,
	}
	if _, ok, err := ReadStateCommitmentCheckpoint(db, 12); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateCommitmentCheckpoint(db, cp); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	if _, ok, err := ReadStateCommitmentDomain(db, stateCommitmentCheckpointLogicalKey(12)); err != nil || !ok {
		t.Fatalf("checkpoint missing from CommitmentDomain: ok=%v err=%v", ok, err)
	}
	got, ok, err := ReadStateCommitmentCheckpoint(db, 12)
	if err != nil || !ok {
		t.Fatalf("read checkpoint = ok:%v err:%v", ok, err)
	}
	if got.BlockNum != cp.BlockNum || got.BlockHash != cp.BlockHash || got.Root != cp.Root || got.Scheme != cp.Scheme {
		t.Fatalf("checkpoint = %+v, want %+v", got, cp)
	}
	if err := WriteStateCommitmentCheckpoint(db, &StateCommitmentCheckpoint{
		BlockNum: 13,
		Root:     common.Hash{0xbb},
		Scheme:   LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("write second checkpoint: %v", err)
	}
	var blocks []uint64
	if err := IterateStateCommitmentCheckpoints(db, func(cp *StateCommitmentCheckpoint) (bool, error) {
		blocks = append(blocks, cp.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate checkpoints: %v", err)
	}
	if len(blocks) != 2 || blocks[0] != 12 || blocks[1] != 13 {
		t.Fatalf("checkpoint blocks = %v, want [12 13]", blocks)
	}
	if err := DeleteStateCommitmentCheckpoint(db, 12); err != nil {
		t.Fatalf("delete checkpoint: %v", err)
	}
	if _, ok, err := ReadStateCommitmentCheckpoint(db, 12); err != nil || ok {
		t.Fatalf("deleted checkpoint ok=%v err=%v", ok, err)
	}
}

func TestStateCommitmentCheckpointMaintainsLatestPointer(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	checkpoints := []*StateCommitmentCheckpoint{
		{BlockNum: 12, BlockHash: common.Hash{0x12}, Root: common.Hash{0xa1}, Scheme: LatestDomainCommitmentScheme},
		{BlockNum: 11, BlockHash: common.Hash{0x11}, Root: common.Hash{0xa0}, Scheme: LatestDomainCommitmentScheme},
		{BlockNum: 13, BlockHash: common.Hash{0x13}, Root: common.Hash{0xa2}, Scheme: LatestDomainCommitmentScheme},
	}
	for i, checkpoint := range checkpoints {
		if err := WriteStateCommitmentCheckpoint(db, checkpoint); err != nil {
			t.Fatalf("write checkpoint %d: %v", checkpoint.BlockNum, err)
		}
		latest, ok, err := ReadLatestStateCommitmentCheckpoint(db)
		if err != nil || !ok {
			t.Fatalf("latest after write %d = ok:%v err:%v", checkpoint.BlockNum, ok, err)
		}
		wantBlock := uint64(12)
		if i == 2 {
			wantBlock = 13
		}
		if latest.BlockNum != wantBlock {
			t.Fatalf("latest after write %d = block %d, want %d", checkpoint.BlockNum, latest.BlockNum, wantBlock)
		}
	}

	var iterated []uint64
	if err := IterateStateCommitmentCheckpoints(db, func(checkpoint *StateCommitmentCheckpoint) (bool, error) {
		iterated = append(iterated, checkpoint.BlockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate checkpoints: %v", err)
	}
	if len(iterated) != 3 || iterated[0] != 11 || iterated[1] != 12 || iterated[2] != 13 {
		t.Fatalf("iterated checkpoints = %v, want [11 12 13]", iterated)
	}

	if err := DeleteStateCommitmentCheckpoint(db, 13); err != nil {
		t.Fatalf("delete latest checkpoint: %v", err)
	}
	latest, ok, err := ReadLatestStateCommitmentCheckpoint(db)
	if err != nil || !ok || latest.BlockNum != 12 {
		t.Fatalf("latest after deleting 13 = block:%v ok:%v err:%v, want 12,true,nil", latest, ok, err)
	}
	if err := DeleteStateCommitmentCheckpoint(db, 12); err != nil {
		t.Fatalf("delete repaired latest checkpoint: %v", err)
	}
	latest, ok, err = ReadLatestStateCommitmentCheckpoint(db)
	if err != nil || !ok || latest.BlockNum != 11 {
		t.Fatalf("latest after deleting 12 = block:%v ok:%v err:%v, want 11,true,nil", latest, ok, err)
	}
	if err := DeleteStateCommitmentCheckpoint(db, 11); err != nil {
		t.Fatalf("delete final checkpoint: %v", err)
	}
	if _, ok, err := ReadLatestStateCommitmentCheckpoint(db); err != nil || ok {
		t.Fatalf("latest after deleting all checkpoints ok=%v err=%v, want false,nil", ok, err)
	}
}

func TestStateCommitmentCheckpointRepairsMissingLatestPointerOnWrite(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for _, checkpoint := range []*StateCommitmentCheckpoint{
		{BlockNum: 12, BlockHash: common.Hash{0x12}, Root: common.Hash{0xa1}, Scheme: LatestDomainCommitmentScheme},
		{BlockNum: 13, BlockHash: common.Hash{0x13}, Root: common.Hash{0xa2}, Scheme: LatestDomainCommitmentScheme},
	} {
		if err := WriteStateCommitmentCheckpoint(db, checkpoint); err != nil {
			t.Fatalf("write checkpoint %d: %v", checkpoint.BlockNum, err)
		}
	}
	if err := DeleteStateCommitmentDomain(db, LatestStateCommitmentCheckpointLogicalKey()); err != nil {
		t.Fatalf("delete latest checkpoint pointer: %v", err)
	}
	if err := WriteStateCommitmentCheckpoint(db, &StateCommitmentCheckpoint{
		BlockNum:  11,
		BlockHash: common.Hash{0x11},
		Root:      common.Hash{0xa0},
		Scheme:    LatestDomainCommitmentScheme,
	}); err != nil {
		t.Fatalf("write lower checkpoint after pointer loss: %v", err)
	}
	latest, ok, err := ReadLatestStateCommitmentCheckpoint(db)
	if err != nil || !ok || latest.BlockNum != 13 {
		t.Fatalf("latest after low write repairs to block:%v ok:%v err:%v, want 13,true,nil", latest, ok, err)
	}
}
