package rawdb

import (
	"bytes"
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
	db := NewMemoryDatabase()
	for n := uint64(1); n <= 3; n++ {
		block := testSyncStagedBlock(n, common.Hash{byte(n - 1)})
		if err := WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
		if n == 3 {
			if err := WriteStageProgressWithHash(db, StageSyncBodies, block.Number(), block.Hash()); err != nil {
				t.Fatalf("write sync bodies progress: %v", err)
			}
			if err := WriteStageProgressWithHash(db, StageSyncBodiesReady, block.Number(), block.Hash()); err != nil {
				t.Fatalf("write sync bodies ready progress: %v", err)
			}
		}
	}

	result := ResetSyncStagedBodies(db)
	if result.StagedDeleteError != nil || result.BodiesProgressError != nil || result.BodiesReadyProgressError != nil {
		t.Fatalf("reset result has error: %+v", result)
	}
	if result.DeletedBodies != 3 {
		t.Fatalf("DeletedBodies = %d, want 3", result.DeletedBodies)
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
