package rawdb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSyncStagedBlockReadWriteDelete(t *testing.T) {
	db := NewMemoryDatabase()
	block := testSyncStagedBlock(3, common.Hash{0x02})

	if _, ok, err := ReadSyncStagedBlock(db, 3); err != nil || ok {
		t.Fatalf("empty staged block ok=%v err=%v", ok, err)
	}
	if err := WriteSyncStagedBlock(db, block); err != nil {
		t.Fatalf("write sync staged block: %v", err)
	}
	got, ok, err := ReadSyncStagedBlock(db, 3)
	if err != nil || !ok || got.Hash() != block.Hash() {
		t.Fatalf("read sync staged block = %v ok=%v err=%v, want %x", got, ok, err, block.Hash())
	}
	if err := DeleteSyncStagedBlock(db, 3); err != nil {
		t.Fatalf("delete sync staged block: %v", err)
	}
	if _, ok, err := ReadSyncStagedBlock(db, 3); err != nil || ok {
		t.Fatalf("deleted staged block ok=%v err=%v", ok, err)
	}
}

func TestReadSyncStagedBlockSurfacesStorageErrors(t *testing.T) {
	if _, ok, err := ReadSyncStagedBlock(stageProgressFailingReader{
		hasErr: errors.New("staged body has failed"),
	}, 3); err == nil || ok || !strings.Contains(err.Error(), "staged body has failed") {
		t.Fatalf("ReadSyncStagedBlock Has failure ok=%v err=%v, want storage error", ok, err)
	}
	if _, ok, err := ReadSyncStagedBlock(stageProgressFailingReader{
		has:    true,
		getErr: errors.New("staged body get failed"),
	}, 3); err == nil || ok || !strings.Contains(err.Error(), "staged body get failed") {
		t.Fatalf("ReadSyncStagedBlock Get failure ok=%v err=%v, want storage error", ok, err)
	}
	if _, ok, err := ReadSyncStagedBlockRaw(stageProgressFailingReader{
		hasErr: errors.New("staged raw has failed"),
	}, 3); err == nil || ok || !strings.Contains(err.Error(), "staged raw has failed") {
		t.Fatalf("ReadSyncStagedBlockRaw Has failure ok=%v err=%v, want storage error", ok, err)
	}
	if _, ok, err := ReadSyncStagedBlockRaw(stageProgressFailingReader{
		has:    true,
		getErr: errors.New("staged raw get failed"),
	}, 3); err == nil || ok || !strings.Contains(err.Error(), "staged raw get failed") {
		t.Fatalf("ReadSyncStagedBlockRaw Get failure ok=%v err=%v, want storage error", ok, err)
	}
}

func TestDeleteSyncStagedBlockBatch(t *testing.T) {
	base := NewMemoryDatabase()
	blocks := []*types.Block{
		testSyncStagedBlock(1, common.Hash{}),
		testSyncStagedBlock(2, common.Hash{0x01}),
		testSyncStagedBlock(3, common.Hash{0x02}),
	}
	for _, block := range blocks {
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}

	db := &countingBatchStore{KeyValueStore: base}
	result := DeleteSyncStagedBlockBatch(db, []SyncStagedBlockDelete{
		{Number: blocks[0].Number(), Hash: blocks[0].Hash()},
		{Number: blocks[1].Number(), Hash: blocks[1].Hash()},
	})
	if result.Deleted != 2 || len(result.Errors) != 0 {
		t.Fatalf("delete result = %+v, want deleted 2 without errors", result)
	}
	if db.batches != 1 || db.directDeletes != 0 {
		t.Fatalf("delete batch used batches=%d directDeletes=%d, want one batch", db.batches, db.directDeletes)
	}
	for _, block := range blocks[:2] {
		if _, ok, err := ReadSyncStagedBlock(db, block.Number()); err != nil || ok {
			t.Fatalf("deleted staged block %d ok=%v err=%v, want missing", block.Number(), ok, err)
		}
	}
	if _, ok, err := ReadSyncStagedBlock(db, blocks[2].Number()); err != nil || !ok {
		t.Fatalf("untouched staged block ok=%v err=%v, want present", ok, err)
	}
}

func TestDeleteSyncStagedBlockBatchNilOrEmpty(t *testing.T) {
	for name, result := range map[string]SyncStagedBlockDeleteResult{
		"nil db":      DeleteSyncStagedBlockBatch(nil, []SyncStagedBlockDelete{{Number: 1}}),
		"empty input": DeleteSyncStagedBlockBatch(NewMemoryDatabase(), nil),
	} {
		if result.Deleted != 0 || len(result.Errors) != 0 {
			t.Fatalf("%s result = %+v, want empty", name, result)
		}
	}
}

func TestWriteSyncImportProgressBatch(t *testing.T) {
	base := NewMemoryDatabase()
	blocks := []*types.Block{
		testSyncStagedBlock(1, common.Hash{}),
		testSyncStagedBlock(2, common.Hash{0x01}),
		testSyncStagedBlock(3, common.Hash{0x02}),
	}
	for _, block := range blocks {
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}

	db := &countingBatchStore{KeyValueStore: base}
	result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
		{Number: blocks[0].Number(), Hash: blocks[0].Hash()},
		{Number: blocks[1].Number(), Hash: blocks[1].Hash()},
	}, []StageProgress{
		{Stage: StageSyncImport, BlockNum: blocks[1].Number(), BlockHash: blocks[1].Hash(), HasBlockHash: true},
		{Stage: StageSyncExecution, BlockNum: blocks[1].Number(), BlockHash: blocks[1].Hash(), HasBlockHash: true},
	})
	if result.Deleted != 2 || len(result.DeleteErrors) != 0 || result.ProgressRows != 2 || result.ProgressError != nil {
		t.Fatalf("result = %+v, want deleted 2 and two progress rows", result)
	}
	if db.batches != 1 || db.directDeletes != 0 || db.directPuts != 0 {
		t.Fatalf("sync import progress used batches=%d directDeletes=%d directPuts=%d, want one batch", db.batches, db.directDeletes, db.directPuts)
	}
	for _, block := range blocks[:2] {
		if _, ok, err := ReadSyncStagedBlock(db, block.Number()); err != nil || ok {
			t.Fatalf("deleted staged block %d ok=%v err=%v, want missing", block.Number(), ok, err)
		}
	}
	if _, ok, err := ReadSyncStagedBlock(db, blocks[2].Number()); err != nil || !ok {
		t.Fatalf("untouched staged block ok=%v err=%v, want present", ok, err)
	}
	for _, stage := range []StageID{StageSyncImport, StageSyncExecution} {
		row, ok, err := ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != blocks[1].Number() || row.BlockHash != blocks[1].Hash() || !row.HasBlockHash {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block2 hash", stage, row, ok, err)
		}
	}
}

func TestWriteSyncImportProgressBatchRejectsInvalidProgressBeforeDeletes(t *testing.T) {
	tests := []struct {
		name string
		rows []StageProgress
		want string
	}{
		{
			name: "unbound sync import",
			rows: []StageProgress{{Stage: StageSyncImport, BlockNum: 2}},
			want: "sync import progress SyncImport at row 0 block 2 is not hash-bound",
		},
		{
			name: "canonical stage",
			rows: []StageProgress{{Stage: StageBodies, BlockNum: 2, BlockHash: common.Hash{0x02}, HasBlockHash: true}},
			want: "unexpected sync import progress stage Bodies at row 0",
		},
		{
			name: "empty stage",
			rows: []StageProgress{{BlockNum: 2, BlockHash: common.Hash{0x02}, HasBlockHash: true}},
			want: "empty stage id at row 0",
		},
		{
			name: "missing upstream import",
			rows: []StageProgress{{Stage: StageSyncExecution, BlockNum: 2, BlockHash: common.Hash{0x02}, HasBlockHash: true}},
			want: "sync import progress row 0 stage SyncExecution, want SyncImport",
		},
		{
			name: "mixed boundaries",
			rows: []StageProgress{
				{Stage: StageSyncImport, BlockNum: 2, BlockHash: common.Hash{0x02}, HasBlockHash: true},
				{Stage: StageSyncExecution, BlockNum: 3, BlockHash: common.Hash{0x03}, HasBlockHash: true},
			},
			want: "sync import progress SyncExecution at row 1 block 3 is ahead of upstream SyncImport block 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := NewMemoryDatabase()
			block := testSyncStagedBlock(2, common.Hash{0x01})
			if err := WriteSyncStagedBlock(base, block); err != nil {
				t.Fatalf("write staged block: %v", err)
			}
			db := &directSyncStageWriter{reader: base, writer: base}

			result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
				{Number: block.Number(), Hash: block.Hash()},
			}, tt.rows)
			if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), tt.want) {
				t.Fatalf("result = %+v, want progress error containing %q", result, tt.want)
			}
			if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
				t.Fatalf("result = %+v, want no delete/progress side effects", result)
			}
			if db.deletes != 0 || db.puts != 0 {
				t.Fatalf("writer side effects deletes=%d puts=%d, want none", db.deletes, db.puts)
			}
			if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
				t.Fatalf("staged block after rejected write ok=%v err=%v, want present", ok, err)
			}
			for _, progress := range tt.rows {
				if progress.Stage == "" {
					continue
				}
				if row, ok, err := ReadStageProgressRow(base, progress.Stage); err != nil || ok {
					t.Fatalf("stage %s row after rejected write = %+v ok=%v err=%v, want absent", progress.Stage, row, ok, err)
				}
			}
		})
	}
}

func TestWriteSyncImportProgressBatchAllowsDownstreamStageLag(t *testing.T) {
	base := NewMemoryDatabase()
	block1 := testSyncStagedBlock(1, common.Hash{})
	block2 := testSyncStagedBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
		{Number: block1.Number(), Hash: block1.Hash()},
		{Number: block2.Number(), Hash: block2.Hash()},
	}, []StageProgress{
		{Stage: StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: StageSyncExecution, BlockNum: block1.Number(), BlockHash: block1.Hash(), HasBlockHash: true},
	})
	if result.Deleted != 2 || len(result.DeleteErrors) != 0 || result.ProgressRows != 2 || result.ProgressError != nil {
		t.Fatalf("result = %+v, want deleted 2 and lagging execution progress", result)
	}
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := ReadSyncStagedBlock(db, block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after import ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
	if row, ok, err := ReadStageProgressRow(db, StageSyncImport); err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
		t.Fatalf("sync import progress = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	if row, ok, err := ReadStageProgressRow(db, StageSyncExecution); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("sync execution progress = %+v ok=%v err=%v, want block1", row, ok, err)
	}
}

func TestWriteSyncImportProgressBatchRejectsExistingProgressRegression(t *testing.T) {
	t.Run("lower block", func(t *testing.T) {
		base := NewMemoryDatabase()
		block1 := testSyncStagedBlock(1, common.Hash{})
		block2 := testSyncStagedBlock(2, block1.Hash())
		block3 := testSyncStagedBlock(3, block2.Hash())
		for _, block := range []*types.Block{block1, block2, block3} {
			if err := WriteSyncStagedBlock(base, block); err != nil {
				t.Fatalf("write staged block %d: %v", block.Number(), err)
			}
		}
		if err := WriteStageProgressWithHash(base, StageSyncImport, block3.Number(), block3.Hash()); err != nil {
			t.Fatalf("write existing sync import progress: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block1.Number(), Hash: block1.Hash()},
			{Number: block2.Number(), Hash: block2.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		})
		if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "would regress existing block 3") {
			t.Fatalf("result = %+v, want existing progress regression error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
			t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || !ok || row.BlockNum != block3.Number() || row.BlockHash != block3.Hash() {
			t.Fatalf("existing sync import progress = %+v ok=%v err=%v, want block3 kept", row, ok, err)
		}
		for _, block := range []*types.Block{block1, block2, block3} {
			if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
				t.Fatalf("staged block %d after rejected write ok=%v err=%v, want present", block.Number(), ok, err)
			}
		}
	})

	t.Run("same height hash mismatch", func(t *testing.T) {
		base := NewMemoryDatabase()
		block := testSyncStagedBlock(2, common.Hash{0x01})
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block: %v", err)
		}
		existingHash := common.Hash{0xee}
		if err := WriteStageProgressWithHash(base, StageSyncImport, block.Number(), existingHash); err != nil {
			t.Fatalf("write existing sync import progress: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block.Number(), Hash: block.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		})
		if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "would replace existing hash") {
			t.Fatalf("result = %+v, want same-height hash replacement error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
			t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || !ok || row.BlockNum != block.Number() || row.BlockHash != existingHash {
			t.Fatalf("existing sync import progress = %+v ok=%v err=%v, want old hash kept", row, ok, err)
		}
		if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
			t.Fatalf("staged block after rejected write ok=%v err=%v, want present", ok, err)
		}
	})
}

func TestWriteSyncImportProgressBatchRejectsMergedProgressOrderViolation(t *testing.T) {
	t.Run("downstream ahead", func(t *testing.T) {
		base := NewMemoryDatabase()
		block1 := testSyncStagedBlock(1, common.Hash{})
		block2 := testSyncStagedBlock(2, block1.Hash())
		block3 := testSyncStagedBlock(3, block2.Hash())
		for _, block := range []*types.Block{block1, block2, block3} {
			if err := WriteSyncStagedBlock(base, block); err != nil {
				t.Fatalf("write staged block %d: %v", block.Number(), err)
			}
		}
		if err := WriteStageProgressWithHash(base, StageSyncExecution, block3.Number(), block3.Hash()); err != nil {
			t.Fatalf("write existing sync execution progress: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block1.Number(), Hash: block1.Hash()},
			{Number: block2.Number(), Hash: block2.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		})
		if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "SyncExecution block 3 is ahead of upstream SyncImport block 2") {
			t.Fatalf("result = %+v, want merged order error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
			t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncExecution); err != nil || !ok || row.BlockNum != block3.Number() || row.BlockHash != block3.Hash() {
			t.Fatalf("existing sync execution progress = %+v ok=%v err=%v, want block3 kept", row, ok, err)
		}
		for _, block := range []*types.Block{block1, block2, block3} {
			if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
				t.Fatalf("staged block %d after rejected write ok=%v err=%v, want present", block.Number(), ok, err)
			}
		}
	})

	t.Run("same height hash mismatch", func(t *testing.T) {
		base := NewMemoryDatabase()
		block := testSyncStagedBlock(2, common.Hash{0x01})
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block: %v", err)
		}
		existingHash := common.Hash{0xee}
		if err := WriteStageProgressWithHash(base, StageSyncExecution, block.Number(), existingHash); err != nil {
			t.Fatalf("write existing sync execution progress: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block.Number(), Hash: block.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		})
		if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "SyncExecution block 2 hash") || !strings.Contains(result.ProgressError.Error(), "does not match upstream SyncImport hash") {
			t.Fatalf("result = %+v, want merged hash mismatch error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncExecution); err != nil || !ok || row.BlockHash != existingHash {
			t.Fatalf("existing sync execution progress = %+v ok=%v err=%v, want old hash kept", row, ok, err)
		}
		if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
			t.Fatalf("staged block after rejected write ok=%v err=%v, want present", ok, err)
		}
	})

	t.Run("missing upstream", func(t *testing.T) {
		base := NewMemoryDatabase()
		block := testSyncStagedBlock(2, common.Hash{0x01})
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block: %v", err)
		}
		if err := WriteStageProgressWithHash(base, StageSyncCommitment, block.Number(), block.Hash()); err != nil {
			t.Fatalf("write existing sync commitment progress: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block.Number(), Hash: block.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		})
		if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "SyncCommitment at block 2 requires upstream SyncExecution") {
			t.Fatalf("result = %+v, want missing upstream error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncCommitment); err != nil || !ok || row.BlockHash != block.Hash() {
			t.Fatalf("existing sync commitment progress = %+v ok=%v err=%v, want kept", row, ok, err)
		}
		if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
			t.Fatalf("staged block after rejected write ok=%v err=%v, want present", ok, err)
		}
	})

	t.Run("legacy unbound downstream", func(t *testing.T) {
		base := NewMemoryDatabase()
		block := testSyncStagedBlock(2, common.Hash{0x01})
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block: %v", err)
		}
		if err := WriteStageProgress(base, StageSyncExecution, block.Number()); err != nil {
			t.Fatalf("write unbound sync execution progress: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block.Number(), Hash: block.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		})
		if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "SyncExecution at block 2 is not hash-bound") {
			t.Fatalf("result = %+v, want legacy unbound downstream error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncExecution); err != nil || !ok || row.HasBlockHash {
			t.Fatalf("existing sync execution progress = %+v ok=%v err=%v, want unbound row kept for startup repair", row, ok, err)
		}
		if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
			t.Fatalf("staged block after rejected write ok=%v err=%v, want present", ok, err)
		}
	})
}

func TestWriteSyncImportProgressBatchAllowsExistingDownstreamLag(t *testing.T) {
	base := NewMemoryDatabase()
	block1 := testSyncStagedBlock(1, common.Hash{})
	block2 := testSyncStagedBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := WriteStageProgressWithHash(base, StageSyncExecution, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write existing sync execution progress: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
		{Number: block1.Number(), Hash: block1.Hash()},
		{Number: block2.Number(), Hash: block2.Hash()},
	}, []StageProgress{
		{Stage: StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	})
	if result.Deleted != 2 || len(result.DeleteErrors) != 0 || result.ProgressRows != 1 || result.ProgressError != nil {
		t.Fatalf("result = %+v, want import advanced while execution lags", result)
	}
	if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
		t.Fatalf("sync import progress = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	if row, ok, err := ReadStageProgressRow(base, StageSyncExecution); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("sync execution progress = %+v ok=%v err=%v, want existing block1", row, ok, err)
	}
}

func TestWriteSyncImportProgressBatchValidatesDeletesBeforeProgress(t *testing.T) {
	t.Run("hash mismatch", func(t *testing.T) {
		base := NewMemoryDatabase()
		block := testSyncStagedBlock(2, common.Hash{0x01})
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block: %v", err)
		}
		db := &countingBatchStore{KeyValueStore: base}
		wrongHash := common.Hash{0xff}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block.Number(), Hash: wrongHash},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		})
		if len(result.DeleteErrors) != 1 || result.DeleteErrors[0].Number != block.Number() || !strings.Contains(result.DeleteErrors[0].Err.Error(), "hash") {
			t.Fatalf("result = %+v, want one delete hash mismatch", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || result.ProgressError != nil {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
			t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
		}
		if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
			t.Fatalf("staged block after rejected delete ok=%v err=%v, want present", ok, err)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || ok {
			t.Fatalf("sync import progress after rejected delete = %+v ok=%v err=%v, want absent", row, ok, err)
		}
	})

	t.Run("missing staged row", func(t *testing.T) {
		base := NewMemoryDatabase()
		db := &directSyncStageWriter{reader: base, writer: base}
		block := testSyncStagedBlock(2, common.Hash{0x01})

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block.Number(), Hash: block.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		})
		if len(result.DeleteErrors) != 1 || result.DeleteErrors[0].Number != block.Number() || !strings.Contains(result.DeleteErrors[0].Err.Error(), "missing") {
			t.Fatalf("result = %+v, want one missing staged row delete error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || result.ProgressError != nil {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if db.deletes != 0 || db.puts != 0 {
			t.Fatalf("writer side effects deletes=%d puts=%d, want none", db.deletes, db.puts)
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || ok {
			t.Fatalf("sync import progress after rejected delete = %+v ok=%v err=%v, want absent", row, ok, err)
		}
	})

	t.Run("gapped delete prefix", func(t *testing.T) {
		base := NewMemoryDatabase()
		block2 := testSyncStagedBlock(2, common.Hash{0x01})
		block4 := testSyncStagedBlock(4, block2.Hash())
		for _, block := range []*types.Block{block2, block4} {
			if err := WriteSyncStagedBlock(base, block); err != nil {
				t.Fatalf("write staged block %d: %v", block.Number(), err)
			}
		}
		db := &countingBatchStore{KeyValueStore: base}

		result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
			{Number: block2.Number(), Hash: block2.Hash()},
			{Number: block4.Number(), Hash: block4.Hash()},
		}, []StageProgress{
			{Stage: StageSyncImport, BlockNum: block4.Number(), BlockHash: block4.Hash(), HasBlockHash: true},
		})
		if len(result.DeleteErrors) != 1 || result.DeleteErrors[0].Number != block4.Number() || !strings.Contains(result.DeleteErrors[0].Err.Error(), "does not continue") {
			t.Fatalf("result = %+v, want one gapped delete prefix error", result)
		}
		if result.Deleted != 0 || result.ProgressRows != 0 || result.ProgressError != nil {
			t.Fatalf("result = %+v, want no delete/progress writes", result)
		}
		if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
			t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
		}
		for _, block := range []*types.Block{block2, block4} {
			if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
				t.Fatalf("staged block %d after rejected delete ok=%v err=%v, want present", block.Number(), ok, err)
			}
		}
		if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || ok {
			t.Fatalf("sync import progress after rejected delete = %+v ok=%v err=%v, want absent", row, ok, err)
		}
	})
}

func TestWriteSyncImportProgressBatchFallbackKeepsBodiesWhenProgressWriteFails(t *testing.T) {
	base := NewMemoryDatabase()
	block := testSyncStagedBlock(2, common.Hash{0x01})
	if err := WriteSyncStagedBlock(base, block); err != nil {
		t.Fatalf("write staged block: %v", err)
	}
	db := &failingSyncImportProgressWriter{store: base, failStage: StageSyncImport}

	result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
		{Number: block.Number(), Hash: block.Hash()},
	}, []StageProgress{
		{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	})
	if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "write stage progress SyncImport") {
		t.Fatalf("result = %+v, want SyncImport progress write error", result)
	}
	if result.Deleted != 0 || len(result.DeleteErrors) != 0 || result.ProgressRows != 0 {
		t.Fatalf("result = %+v, want no body delete after progress failure", result)
	}
	if db.deletes != 0 {
		t.Fatalf("delete attempts = %d, want none before progress succeeds", db.deletes)
	}
	if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
		t.Fatalf("staged block after progress failure ok=%v err=%v, want present", ok, err)
	}
	if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || ok {
		t.Fatalf("sync import progress after failed write = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestWriteSyncImportProgressBatchFallbackPublishesProgressBeforeDeleteFailure(t *testing.T) {
	base := NewMemoryDatabase()
	block := testSyncStagedBlock(2, common.Hash{0x01})
	if err := WriteSyncStagedBlock(base, block); err != nil {
		t.Fatalf("write staged block: %v", err)
	}
	db := &failingSyncImportDeleteWriter{store: base, failNumber: block.Number()}

	result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
		{Number: block.Number(), Hash: block.Hash()},
	}, []StageProgress{
		{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	})
	if len(result.DeleteErrors) != 1 || result.DeleteErrors[0].Number != block.Number() {
		t.Fatalf("result = %+v, want staged body delete error", result)
	}
	if result.ProgressError != nil || result.ProgressRows != 1 || result.Deleted != 0 {
		t.Fatalf("result = %+v, want published progress and failed delete", result)
	}
	if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || !ok || row.BlockNum != block.Number() || row.BlockHash != block.Hash() || !row.HasBlockHash {
		t.Fatalf("sync import progress after delete failure = %+v ok=%v err=%v, want block2 hash", row, ok, err)
	}
	if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
		t.Fatalf("staged block after delete failure ok=%v err=%v, want still present", ok, err)
	}
}

func TestWriteSyncImportProgressBatchRejectsProgressDeleteBoundaryMismatch(t *testing.T) {
	base := NewMemoryDatabase()
	block2 := testSyncStagedBlock(2, common.Hash{0x01})
	block3 := testSyncStagedBlock(3, block2.Hash())
	if err := WriteSyncStagedBlock(base, block2); err != nil {
		t.Fatalf("write staged block2: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncImportProgressBatch(db, []SyncStagedBlockDelete{
		{Number: block2.Number(), Hash: block2.Hash()},
	}, []StageProgress{
		{Stage: StageSyncImport, BlockNum: block3.Number(), BlockHash: block3.Hash(), HasBlockHash: true},
	})
	if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "outside staged delete prefix") {
		t.Fatalf("result = %+v, want progress outside staged delete prefix", result)
	}
	if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
		t.Fatalf("result = %+v, want no delete/progress writes", result)
	}
	if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
		t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
	}
	if _, ok, err := ReadSyncStagedBlock(base, block2.Number()); err != nil || !ok {
		t.Fatalf("staged block2 after rejected write ok=%v err=%v, want present", ok, err)
	}
	if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || ok {
		t.Fatalf("sync import progress after rejected write = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestWriteSyncImportProgressBatchRejectsProgressWithoutDeleteProof(t *testing.T) {
	base := NewMemoryDatabase()
	block := testSyncStagedBlock(2, common.Hash{0x01})
	if err := WriteSyncStagedBlock(base, block); err != nil {
		t.Fatalf("write staged block: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncImportProgressBatch(db, nil, []StageProgress{
		{Stage: StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	})
	if result.ProgressError == nil || !strings.Contains(result.ProgressError.Error(), "has no staged delete prefix") {
		t.Fatalf("result = %+v, want progress-without-delete proof error", result)
	}
	if result.Deleted != 0 || result.ProgressRows != 0 || len(result.DeleteErrors) != 0 {
		t.Fatalf("result = %+v, want no delete/progress writes", result)
	}
	if db.batches != 0 || db.directDeletes != 0 || db.directPuts != 0 {
		t.Fatalf("writer side effects batches=%d deletes=%d puts=%d, want none", db.batches, db.directDeletes, db.directPuts)
	}
	if _, ok, err := ReadSyncStagedBlock(base, block.Number()); err != nil || !ok {
		t.Fatalf("staged block after rejected progress-only write ok=%v err=%v, want present", ok, err)
	}
	if row, ok, err := ReadStageProgressRow(base, StageSyncImport); err != nil || ok {
		t.Fatalf("sync import progress after rejected progress-only write = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestSyncStagedBlockRawIterate(t *testing.T) {
	db := NewMemoryDatabase()
	for _, n := range []uint64{4, 2, 3} {
		block := testSyncStagedBlock(n, common.Hash{byte(n - 1)})
		raw, err := block.Marshal()
		if err != nil {
			t.Fatalf("marshal block %d: %v", n, err)
		}
		if err := WriteSyncStagedBlockRaw(db, block, raw); err != nil {
			t.Fatalf("write raw staged block %d: %v", n, err)
		}
	}
	row, ok, err := ReadSyncStagedBlockRaw(db, 2)
	if err != nil || !ok || row.Number != 2 || row.Hash != testSyncStagedBlock(2, common.Hash{0x01}).Hash() {
		t.Fatalf("ReadSyncStagedBlockRaw = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	if len(row.Raw) == 0 {
		t.Fatal("ReadSyncStagedBlockRaw returned empty raw bytes")
	}
	row.Raw[0] ^= 0xff
	row2, ok, err := ReadSyncStagedBlockRaw(db, 2)
	if err != nil || !ok {
		t.Fatalf("second ReadSyncStagedBlockRaw ok=%v err=%v", ok, err)
	}
	if bytes.Equal(row.Raw, row2.Raw) {
		t.Fatal("ReadSyncStagedBlockRaw returned aliased bytes")
	}

	var got []uint64
	err = IterateSyncStagedBlocksFrom(db, 3, func(row SyncStagedBlockRow) (bool, error) {
		got = append(got, row.Number)
		if row.Hash == (common.Hash{}) || len(row.Raw) == 0 {
			t.Fatalf("iter row missing hash/raw: %+v", row)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("IterateSyncStagedBlocksFrom: %v", err)
	}
	if want := []uint64{3, 4}; !equalUint64s(got, want) {
		t.Fatalf("iterated staged blocks = %v, want %v", got, want)
	}
}

func TestWriteSyncStagedBlockRawAndProgressWritesBodyAndProgress(t *testing.T) {
	base := NewMemoryDatabase()
	db := &countingBatchStore{KeyValueStore: base}
	block := testSyncStagedBlock(3, common.Hash{0x02})
	raw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}

	result := WriteSyncStagedBlockRawAndProgress(db, block, raw)
	if result.StageError != nil || result.ProgressReadError != nil || result.ProgressWriteError != nil {
		t.Fatalf("write result has error: %+v", result)
	}
	if !result.Staged || !result.ProgressWritten || result.ProgressSkipped {
		t.Fatalf("write result = %+v, want staged progress write", result)
	}
	if db.batches != 1 || db.directPuts != 0 {
		t.Fatalf("writes used batches=%d directPuts=%d, want one batch and no direct puts", db.batches, db.directPuts)
	}
	row, ok, err := ReadSyncStagedBlockRaw(db, block.Number())
	if err != nil || !ok || row.Hash != block.Hash() || !bytes.Equal(row.Raw, raw) {
		t.Fatalf("staged row = %+v ok=%v err=%v, want block raw", row, ok, err)
	}
	progress, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil || !ok || progress.BlockNum != block.Number() || progress.BlockHash != block.Hash() {
		t.Fatalf("sync bodies progress = %+v ok=%v err=%v, want block3", progress, ok, err)
	}
}

func TestWriteSyncStagedBlockRawAndProgressRejectsMismatchedRaw(t *testing.T) {
	db := NewMemoryDatabase()
	block2 := testSyncStagedBlock(2, common.Hash{0x01})
	block3 := testSyncStagedBlock(3, block2.Hash())
	raw3, err := block3.Marshal()
	if err != nil {
		t.Fatalf("marshal block3: %v", err)
	}

	result := WriteSyncStagedBlockRawAndProgress(db, block2, raw3)
	if result.StageError == nil || !strings.Contains(result.StageError.Error(), "sync staged raw key 2 contains block 3") {
		t.Fatalf("write mismatched raw result = %+v, want staged raw mismatch error", result)
	}
	if result.Staged || result.ProgressWritten || result.ProgressSkipped {
		t.Fatalf("write mismatched raw result = %+v, want no staged/progress side effects", result)
	}
	if _, ok, err := ReadSyncStagedBlockRaw(db, block2.Number()); err != nil || ok {
		t.Fatalf("mismatched raw left staged row ok=%v err=%v, want absent", ok, err)
	}
	if _, ok, err := ReadStageProgressRow(db, StageSyncBodies); err != nil || ok {
		t.Fatalf("mismatched raw wrote progress ok=%v err=%v, want absent", ok, err)
	}
}

func TestWriteSyncStagedBlockRawAndProgressDoesNotRegressProgress(t *testing.T) {
	base := NewMemoryDatabase()
	block3 := testSyncStagedBlock(3, common.Hash{0x02})
	block5 := testSyncStagedBlock(5, common.Hash{0x04})
	if err := WriteStageProgressWithHash(base, StageSyncBodies, block5.Number(), block5.Hash()); err != nil {
		t.Fatalf("write existing progress: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncStagedBlockRawAndProgress(db, block3, nil)
	if result.StageError != nil || result.ProgressReadError != nil || result.ProgressWriteError != nil {
		t.Fatalf("write result has error: %+v", result)
	}
	if !result.Staged || !result.HadPreviousProgress || !result.ProgressSkipped || result.ProgressWritten {
		t.Fatalf("write result = %+v, want staged and skipped progress", result)
	}
	if db.batches != 0 || db.directPuts != 1 {
		t.Fatalf("writes used batches=%d directPuts=%d, want one direct body put", db.batches, db.directPuts)
	}
	if _, ok, err := ReadSyncStagedBlock(db, block3.Number()); err != nil || !ok {
		t.Fatalf("staged block3 ok=%v err=%v, want present", ok, err)
	}
	progress, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil || !ok || progress.BlockNum != block5.Number() || progress.BlockHash != block5.Hash() {
		t.Fatalf("sync bodies progress = %+v ok=%v err=%v, want existing block5", progress, ok, err)
	}
}

func TestWriteSyncStagedBlockRawAndProgressRejectsSameHeightProgressHashMismatch(t *testing.T) {
	base := NewMemoryDatabase()
	original := testSyncStagedBlock(3, common.Hash{0x02})
	replacement := testSyncStagedBlock(3, common.Hash{0xff})
	if original.Hash() == replacement.Hash() {
		t.Fatal("test blocks unexpectedly share a hash")
	}
	if err := WriteSyncStagedBlock(base, original); err != nil {
		t.Fatalf("write original staged block: %v", err)
	}
	if err := WriteStageProgressWithHash(base, StageSyncBodies, original.Number(), original.Hash()); err != nil {
		t.Fatalf("write existing SyncBodies progress: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncStagedBlockRawAndProgress(db, replacement, nil)
	if result.StageError == nil || !strings.Contains(result.StageError.Error(), "conflicts staged block hash") {
		t.Fatalf("write conflict result = %+v, want same-height progress hash conflict", result)
	}
	if result.Staged || result.ProgressWritten || result.ProgressSkipped || result.ProgressWriteError != nil || result.ProgressReadError != nil {
		t.Fatalf("write conflict result = %+v, want no body/progress side effects", result)
	}
	if db.batches != 0 || db.directPuts != 0 {
		t.Fatalf("writes used batches=%d directPuts=%d, want no writes", db.batches, db.directPuts)
	}
	row, ok, err := ReadSyncStagedBlockRaw(db, original.Number())
	if err != nil || !ok || row.Hash != original.Hash() {
		t.Fatalf("staged row after conflict = %+v ok=%v err=%v, want original hash", row, ok, err)
	}
	progress, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil || !ok || progress.BlockNum != original.Number() || progress.BlockHash != original.Hash() {
		t.Fatalf("SyncBodies after conflict = %+v ok=%v err=%v, want original progress", progress, ok, err)
	}
}

func TestWriteSyncStagedBlockRawAndProgressRejectsSameNumberStagedHashMismatch(t *testing.T) {
	base := NewMemoryDatabase()
	original := testSyncStagedBlock(3, common.Hash{0x02})
	replacement := testSyncStagedBlock(3, common.Hash{0xff})
	if original.Hash() == replacement.Hash() {
		t.Fatal("test blocks unexpectedly share a hash")
	}
	if err := WriteSyncStagedBlock(base, original); err != nil {
		t.Fatalf("write original staged block: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncStagedBlockRawAndProgress(db, replacement, nil)
	if result.StageError == nil || !strings.Contains(result.StageError.Error(), "sync staged block 3 hash") {
		t.Fatalf("write staged conflict result = %+v, want same-number staged hash conflict", result)
	}
	if result.Staged || result.ProgressWritten || result.ProgressSkipped || result.ProgressReadError != nil || result.ProgressWriteError != nil {
		t.Fatalf("write staged conflict result = %+v, want no body/progress side effects", result)
	}
	if db.batches != 0 || db.directPuts != 0 {
		t.Fatalf("writes used batches=%d directPuts=%d, want no writes", db.batches, db.directPuts)
	}
	row, ok, err := ReadSyncStagedBlockRaw(db, original.Number())
	if err != nil || !ok || row.Hash != original.Hash() {
		t.Fatalf("staged row after conflict = %+v ok=%v err=%v, want original hash", row, ok, err)
	}
	if progress, ok, err := ReadStageProgressRow(db, StageSyncBodies); err != nil || ok {
		t.Fatalf("SyncBodies after conflict = %+v ok=%v err=%v, want absent", progress, ok, err)
	}
}

func TestWriteSyncStagedBlockRawAndProgressReplacesUnboundProgress(t *testing.T) {
	base := NewMemoryDatabase()
	block3 := testSyncStagedBlock(3, common.Hash{0x02})
	if err := WriteStageProgress(base, StageSyncBodies, 5); err != nil {
		t.Fatalf("write legacy unbound progress: %v", err)
	}
	db := &countingBatchStore{KeyValueStore: base}

	result := WriteSyncStagedBlockRawAndProgress(db, block3, nil)
	if result.StageError != nil || result.ProgressReadError != nil || result.ProgressWriteError != nil {
		t.Fatalf("write result has error: %+v", result)
	}
	if !result.Staged || !result.HadPreviousProgress || result.ProgressSkipped || !result.ProgressWritten {
		t.Fatalf("write result = %+v, want staged and hash-bound progress rewrite", result)
	}
	if db.batches != 1 || db.directPuts != 0 {
		t.Fatalf("writes used batches=%d directPuts=%d, want one batch", db.batches, db.directPuts)
	}
	progress, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil || !ok || progress.BlockNum != block3.Number() || progress.BlockHash != block3.Hash() || !progress.HasBlockHash {
		t.Fatalf("sync bodies progress = %+v ok=%v err=%v, want hash-bound block3", progress, ok, err)
	}
}

func TestWriteSyncStagedBlockRawAndProgressStagesBodyOnProgressReadError(t *testing.T) {
	db := NewMemoryDatabase()
	block := testSyncStagedBlock(3, common.Hash{0x02})
	if err := db.Put(stageProgressKey(StageSyncBodies), []byte{0x01}); err != nil {
		t.Fatalf("write corrupt progress: %v", err)
	}

	result := WriteSyncStagedBlockRawAndProgress(db, block, nil)
	if result.StageError != nil || result.ProgressReadError == nil || result.ProgressWriteError != nil {
		t.Fatalf("write result = %+v, want progress read error only", result)
	}
	if !result.Staged || result.ProgressWritten || result.ProgressSkipped {
		t.Fatalf("write result = %+v, want staged without progress update", result)
	}
	if _, ok, err := ReadSyncStagedBlock(db, block.Number()); err != nil || !ok {
		t.Fatalf("staged block ok=%v err=%v, want present", ok, err)
	}
}

func TestDeleteSyncStagedBlocksThrough(t *testing.T) {
	base := NewMemoryDatabase()
	for n := uint64(1); n <= 4; n++ {
		if err := WriteSyncStagedBlock(base, testSyncStagedBlock(n, common.Hash{byte(n - 1)})); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
	}
	db := &countingBatchStore{KeyValueStore: base}
	deleted, err := DeleteSyncStagedBlocksThrough(db, 2)
	if err != nil {
		t.Fatalf("delete staged blocks through: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted staged blocks = %d, want 2", deleted)
	}
	if db.batches != 1 || db.directDeletes != 0 {
		t.Fatalf("delete through used batches=%d directDeletes=%d, want one batch", db.batches, db.directDeletes)
	}
	for n := uint64(1); n <= 4; n++ {
		_, ok, err := ReadSyncStagedBlock(db, n)
		if err != nil {
			t.Fatalf("read staged block %d: %v", n, err)
		}
		if n <= 2 && ok {
			t.Fatalf("staged block %d survived cleanup", n)
		}
		if n > 2 && !ok {
			t.Fatalf("staged block %d was deleted unexpectedly", n)
		}
	}
}

func TestDeleteSyncStagedBlocksFrom(t *testing.T) {
	base := NewMemoryDatabase()
	for n := uint64(1); n <= 4; n++ {
		if err := WriteSyncStagedBlock(base, testSyncStagedBlock(n, common.Hash{byte(n - 1)})); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
	}
	db := &countingBatchStore{KeyValueStore: base}
	deleted, err := DeleteSyncStagedBlocksFrom(db, 3)
	if err != nil {
		t.Fatalf("delete staged blocks from: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted staged blocks = %d, want 2", deleted)
	}
	if db.batches != 1 || db.directDeletes != 0 {
		t.Fatalf("delete from used batches=%d directDeletes=%d, want one batch", db.batches, db.directDeletes)
	}
	for n := uint64(1); n <= 4; n++ {
		_, ok, err := ReadSyncStagedBlock(db, n)
		if err != nil {
			t.Fatalf("read staged block %d: %v", n, err)
		}
		if n < 3 && !ok {
			t.Fatalf("staged block %d was deleted unexpectedly", n)
		}
		if n >= 3 && ok {
			t.Fatalf("staged block %d survived cleanup", n)
		}
	}
}

func TestPruneSyncStagedBlocksFromRewindsBodiesProgress(t *testing.T) {
	db := NewMemoryDatabase()
	block2 := testSyncStagedBlock(2, common.Hash{0x01})
	block3 := testSyncStagedBlock(3, block2.Hash())
	block4 := testSyncStagedBlock(4, block3.Hash())
	for _, block := range []*types.Block{block2, block3, block4} {
		if err := WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := WriteStageProgressWithHash(db, StageSyncBodies, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}

	result, err := PruneSyncStagedBlocksFrom(db, 3, block2.Number(), block2.Hash(), true)
	if err != nil {
		t.Fatalf("prune sync staged blocks: %v", err)
	}
	if result.Deleted != 2 || !result.HadProgress || !result.RewoundProgress || result.RewindBlock != block2.Number() || result.RewindHash != block2.Hash() {
		t.Fatalf("result = %+v, want deleted 2 and rewound to block2", result)
	}
	for _, n := range []uint64{3, 4} {
		if _, ok, err := ReadSyncStagedBlock(db, n); err != nil || ok {
			t.Fatalf("staged block %d after prune ok=%v err=%v, want deleted", n, ok, err)
		}
	}
	row, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
		t.Fatalf("sync bodies progress = %+v ok=%v err=%v, want block2", row, ok, err)
	}
}

func TestPruneSyncStagedBlocksFromDeletesBodiesProgressWithoutRestoredBlock(t *testing.T) {
	db := NewMemoryDatabase()
	block4 := testSyncStagedBlock(4, common.Hash{0x03})
	if err := WriteSyncStagedBlock(db, block4); err != nil {
		t.Fatalf("write staged block: %v", err)
	}
	if err := WriteStageProgressWithHash(db, StageSyncBodies, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}
	if err := WriteStageProgressWithHash(db, StageSyncBodiesReady, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies ready progress: %v", err)
	}

	result, err := PruneSyncStagedBlocksFrom(db, 2, 0, common.Hash{}, false)
	if err != nil {
		t.Fatalf("prune sync staged blocks: %v", err)
	}
	if result.Deleted != 1 || !result.HadProgress || !result.DeletedProgress || result.RewoundProgress {
		t.Fatalf("result = %+v, want deleted block and bodies progress", result)
	}
	if _, ok, err := ReadSyncStagedBlock(db, block4.Number()); err != nil || ok {
		t.Fatalf("staged block after prune ok=%v err=%v, want deleted", ok, err)
	}
	if row, ok, err := ReadStageProgressRow(db, StageSyncBodies); err != nil || ok {
		t.Fatalf("sync bodies progress after prune = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
	if row, ok, err := ReadStageProgressRow(db, StageSyncBodiesReady); err != nil || !ok || row.BlockNum != block4.Number() {
		t.Fatalf("sync bodies ready progress = %+v ok=%v err=%v, want unchanged rawdb row", row, ok, err)
	}
}

func TestPruneSyncStagedBlocksFromKeepsBodiesOnProgressReadError(t *testing.T) {
	db := NewMemoryDatabase()
	block2 := testSyncStagedBlock(2, common.Hash{0x01})
	block3 := testSyncStagedBlock(3, block2.Hash())
	block4 := testSyncStagedBlock(4, block3.Hash())
	for _, block := range []*types.Block{block2, block3, block4} {
		if err := WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := db.Put(stageProgressKey(StageSyncBodies), []byte{0x01}); err != nil {
		t.Fatalf("write corrupt SyncBodies progress: %v", err)
	}

	result, err := PruneSyncStagedBlocksFrom(db, 3, block2.Number(), block2.Hash(), true)
	if err == nil || !strings.Contains(err.Error(), `stage progress "SyncBodies"`) {
		t.Fatalf("prune result=%+v err=%v, want SyncBodies progress read error", result, err)
	}
	if result.Deleted != 0 || result.DeletedProgress || result.RewoundProgress {
		t.Fatalf("prune result=%+v, want no side-effect counters after read error", result)
	}
	for _, block := range []*types.Block{block2, block3, block4} {
		if got, ok, readErr := ReadSyncStagedBlock(db, block.Number()); readErr != nil || !ok || got.Hash() != block.Hash() {
			t.Fatalf("staged block %d after failed prune = %v ok=%v err=%v, want kept", block.Number(), got, ok, readErr)
		}
	}
}

func TestDeleteAllSyncStagedBlocks(t *testing.T) {
	base := NewMemoryDatabase()
	for n := uint64(1); n <= 3; n++ {
		if err := WriteSyncStagedBlock(base, testSyncStagedBlock(n, common.Hash{byte(n - 1)})); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
	}
	db := &countingBatchStore{KeyValueStore: base}
	deleted, err := DeleteAllSyncStagedBlocks(db)
	if err != nil {
		t.Fatalf("delete all staged blocks: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("deleted staged blocks = %d, want 3", deleted)
	}
	if db.batches != 1 || db.directDeletes != 0 {
		t.Fatalf("delete all used batches=%d directDeletes=%d, want one batch", db.batches, db.directDeletes)
	}
	for n := uint64(1); n <= 3; n++ {
		if _, ok, err := ReadSyncStagedBlock(db, n); err != nil || ok {
			t.Fatalf("staged block %d after delete all ok=%v err=%v", n, ok, err)
		}
	}
}

func TestResetSyncStagedBodiesDeletesRowsAndProgress(t *testing.T) {
	base := NewMemoryDatabase()
	for n := uint64(1); n <= 3; n++ {
		block := testSyncStagedBlock(n, common.Hash{byte(n - 1)})
		if err := WriteSyncStagedBlock(base, block); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
		if n == 3 {
			if err := WriteStageProgressWithHash(base, StageSyncBodies, block.Number(), block.Hash()); err != nil {
				t.Fatalf("write sync bodies progress: %v", err)
			}
			if err := WriteStageProgressWithHash(base, StageSyncBodiesReady, block.Number(), block.Hash()); err != nil {
				t.Fatalf("write sync bodies ready progress: %v", err)
			}
		}
	}

	db := &countingBatchStore{KeyValueStore: base}
	result := ResetSyncStagedBodies(db)
	if result.StagedDeleteError != nil || result.BodiesProgressError != nil || result.BodiesReadyProgressError != nil {
		t.Fatalf("reset result has error: %+v", result)
	}
	if result.DeletedBodies != 3 {
		t.Fatalf("DeletedBodies = %d, want 3", result.DeletedBodies)
	}
	if db.batches != 1 || db.directDeletes != 0 {
		t.Fatalf("reset used batches=%d directDeletes=%d, want one batch", db.batches, db.directDeletes)
	}
	for n := uint64(1); n <= 3; n++ {
		if _, ok, err := ReadSyncStagedBlock(db, n); err != nil || ok {
			t.Fatalf("staged block %d after reset ok=%v err=%v, want deleted", n, ok, err)
		}
	}
	if row, ok, err := ReadStageProgressRow(db, StageSyncBodies); err != nil || ok {
		t.Fatalf("sync bodies progress after reset = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
	if row, ok, err := ReadStageProgressRow(db, StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("sync bodies ready progress after reset = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func testSyncStagedBlock(number uint64, parent common.Hash) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     int64(number),
				Timestamp:  int64(number) * 3000,
				ParentHash: parent.Bytes(),
			},
			WitnessSignature: make([]byte, 65),
		},
	})
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type countingBatchStore struct {
	ethdb.KeyValueStore

	directPuts    int
	directDeletes int
	batches       int
}

func (db *countingBatchStore) Put(key []byte, value []byte) error {
	db.directPuts++
	return db.KeyValueStore.Put(key, value)
}

func (db *countingBatchStore) Delete(key []byte) error {
	db.directDeletes++
	return db.KeyValueStore.Delete(key)
}

func (db *countingBatchStore) NewBatch() ethdb.Batch {
	db.batches++
	return db.KeyValueStore.NewBatch()
}

func (db *countingBatchStore) NewBatchWithSize(size int) ethdb.Batch {
	db.batches++
	return db.KeyValueStore.NewBatchWithSize(size)
}

type directSyncStageWriter struct {
	reader  ethdb.KeyValueReader
	writer  ethdb.KeyValueWriter
	puts    int
	deletes int
}

func (db *directSyncStageWriter) Has(key []byte) (bool, error) {
	return db.reader.Has(key)
}

func (db *directSyncStageWriter) Get(key []byte) ([]byte, error) {
	return db.reader.Get(key)
}

func (db *directSyncStageWriter) Put(key []byte, value []byte) error {
	db.puts++
	return db.writer.Put(key, value)
}

func (db *directSyncStageWriter) Delete(key []byte) error {
	db.deletes++
	return db.writer.Delete(key)
}

type failingSyncImportProgressWriter struct {
	store     ethdb.KeyValueStore
	failStage StageID
	deletes   int
}

func (db *failingSyncImportProgressWriter) Has(key []byte) (bool, error) {
	return db.store.Has(key)
}

func (db *failingSyncImportProgressWriter) Get(key []byte) ([]byte, error) {
	return db.store.Get(key)
}

func (db *failingSyncImportProgressWriter) Put(key []byte, value []byte) error {
	if bytes.Equal(key, stageProgressKey(db.failStage)) {
		return errors.New("boom")
	}
	return db.store.Put(key, value)
}

func (db *failingSyncImportProgressWriter) Delete(key []byte) error {
	db.deletes++
	return db.store.Delete(key)
}

type failingSyncImportDeleteWriter struct {
	store      ethdb.KeyValueStore
	failNumber uint64
}

func (db *failingSyncImportDeleteWriter) Has(key []byte) (bool, error) {
	return db.store.Has(key)
}

func (db *failingSyncImportDeleteWriter) Get(key []byte) ([]byte, error) {
	return db.store.Get(key)
}

func (db *failingSyncImportDeleteWriter) Put(key []byte, value []byte) error {
	return db.store.Put(key, value)
}

func (db *failingSyncImportDeleteWriter) Delete(key []byte) error {
	if bytes.Equal(key, syncStagedBlockKey(db.failNumber)) {
		return errors.New("boom")
	}
	return db.store.Delete(key)
}
