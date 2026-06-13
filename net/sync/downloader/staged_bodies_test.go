package downloader

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestFindStagedBodyReadyFrontierContiguous(t *testing.T) {
	rows := map[uint64]rawdb.SyncStagedBlockRow{
		3: {Number: 3, Hash: tcommon.Hash{0x03}},
		4: {Number: 4, Hash: tcommon.Hash{0x04}},
		5: {Number: 5, Hash: tcommon.Hash{0x05}},
	}
	got := FindStagedBodyReadyFrontier(3, 0, stagedBodyMapReader(rows))
	if !got.Have || got.Number != 5 || got.Hash != (tcommon.Hash{0x05}) || got.NextMissing != 6 || got.Error != nil {
		t.Fatalf("frontier = %+v, want block5 next6 without error", got)
	}
}

func TestFindStagedBodyReadyFrontierStopsAtGap(t *testing.T) {
	rows := map[uint64]rawdb.SyncStagedBlockRow{
		3: {Number: 3, Hash: tcommon.Hash{0x03}},
		5: {Number: 5, Hash: tcommon.Hash{0x05}},
	}
	got := FindStagedBodyReadyFrontier(3, 0, stagedBodyMapReader(rows))
	if !got.Have || got.Number != 3 || got.NextMissing != 4 || got.Error != nil {
		t.Fatalf("frontier = %+v, want block3 next4 gap", got)
	}
}

func TestFindStagedBodyReadyFrontierHonorsTargetHead(t *testing.T) {
	rows := map[uint64]rawdb.SyncStagedBlockRow{
		3: {Number: 3, Hash: tcommon.Hash{0x03}},
		4: {Number: 4, Hash: tcommon.Hash{0x04}},
		5: {Number: 5, Hash: tcommon.Hash{0x05}},
	}
	got := FindStagedBodyReadyFrontier(3, 4, stagedBodyMapReader(rows))
	if !got.Have || got.Number != 4 || got.Hash != (tcommon.Hash{0x04}) || got.NextMissing != 5 {
		t.Fatalf("frontier = %+v, want block4 capped before 5", got)
	}
}

func TestFindStagedBodyReadyFrontierKeepsPrefixOnReadError(t *testing.T) {
	readErr := errors.New("boom")
	rows := map[uint64]rawdb.SyncStagedBlockRow{
		3: {Number: 3, Hash: tcommon.Hash{0x03}},
	}
	got := FindStagedBodyReadyFrontier(3, 0, func(number uint64) (rawdb.SyncStagedBlockRow, bool, error) {
		if number == 4 {
			return rawdb.SyncStagedBlockRow{}, false, readErr
		}
		return stagedBodyMapReader(rows)(number)
	})
	if !got.Have || got.Number != 3 || got.ErrorAt != 4 || !errors.Is(got.Error, readErr) || got.NextMissing != 4 {
		t.Fatalf("frontier = %+v, want block3 with error at 4", got)
	}
}

func TestFindStagedBodyReadyFrontierWithoutReader(t *testing.T) {
	got := FindStagedBodyReadyFrontier(7, 0, nil)
	if got.Have || got.NextMissing != 7 || got.Error != nil {
		t.Fatalf("frontier = %+v, want empty next7", got)
	}
}

func TestRefreshStagedBodyReadyProgressWritesContiguousFrontier(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block4 := testBufferedBlock(4)
	for _, block := range []*types.Block{block2, block3, block4} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}

	got := RefreshStagedBodyReadyProgress(db, 2, 3)
	if !got.Updated || got.Deleted || got.WriteError != nil || got.DeleteError != nil {
		t.Fatalf("refresh result = %+v, want update only", got)
	}
	if !got.Frontier.Have || got.Frontier.Number != 3 || got.Frontier.Hash != block3.Hash() {
		t.Fatalf("frontier = %+v, want block3 capped by target", got.Frontier)
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil || !ok || row.BlockNum != block3.Number() || row.BlockHash != block3.Hash() {
		t.Fatalf("ready progress = %+v ok=%v err=%v, want block3", row, ok, err)
	}
}

func TestRefreshStagedBodyReadyProgressDeletesWhenNoFrontier(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, 9, tcommon.Hash{0x09}); err != nil {
		t.Fatalf("write stale ready progress: %v", err)
	}

	got := RefreshStagedBodyReadyProgress(db, 2, 0)
	if !got.Deleted || got.Updated || got.DeleteError != nil || got.Frontier.Have {
		t.Fatalf("refresh result = %+v, want delete without frontier", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("ready progress after delete = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestRefreshStagedBodyReadyProgressKeepsPrefixOnReadError(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	if err := rawdb.WriteSyncStagedBlock(db, block2); err != nil {
		t.Fatalf("write staged block2: %v", err)
	}
	if err := rawdb.WriteSyncStagedBlockRaw(db, block3, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("write corrupt staged block3: %v", err)
	}

	got := RefreshStagedBodyReadyProgress(db, 2, 0)
	if got.Frontier.Error == nil || got.Frontier.ErrorAt != 3 {
		t.Fatalf("frontier error = %+v, want read error at 3", got.Frontier)
	}
	if !got.Updated || got.WriteError != nil || !got.Frontier.Have || got.Frontier.Number != 2 || got.Frontier.Hash != block2.Hash() {
		t.Fatalf("refresh result = %+v, want prefix update to block2", got)
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
		t.Fatalf("ready progress = %+v ok=%v err=%v, want block2", row, ok, err)
	}
}

func TestRefreshStagedBodyReadyProgressDeletesOnFirstReadError(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, 9, tcommon.Hash{0x09}); err != nil {
		t.Fatalf("write stale ready progress: %v", err)
	}
	if err := rawdb.WriteSyncStagedBlockRaw(db, block2, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("write corrupt staged block2: %v", err)
	}

	got := RefreshStagedBodyReadyProgress(db, 2, 0)
	if got.Frontier.Error == nil || got.Frontier.ErrorAt != 2 {
		t.Fatalf("frontier error = %+v, want read error at 2", got.Frontier)
	}
	if !got.Deleted || got.Updated || got.DeleteError != nil || got.Frontier.Have {
		t.Fatalf("refresh result = %+v, want delete on first read error", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("ready progress after first-error delete = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestValidateStagedBodyReadyDrainLimit(t *testing.T) {
	readErr := errors.New("read staged")
	row := rawdb.StageProgress{
		Stage:        rawdb.StageSyncBodiesReady,
		BlockNum:     7,
		BlockHash:    tcommon.Hash{0x07},
		HasBlockHash: true,
	}
	staged := rawdb.SyncStagedBlockRow{Number: 7, Hash: row.BlockHash}
	tests := []struct {
		name       string
		next       uint64
		row        rawdb.StageProgress
		haveRow    bool
		staged     rawdb.SyncStagedBlockRow
		haveStaged bool
		readErr    error
		status     StagedBodyReadyLimitStatus
		valid      bool
		limit      uint64
	}{
		{name: "missing", next: 7, status: StagedBodyReadyLimitMissing},
		{name: "unbound", next: 7, row: rawdb.StageProgress{BlockNum: 7}, haveRow: true, status: StagedBodyReadyLimitUnbound},
		{name: "stale", next: 8, row: row, haveRow: true, status: StagedBodyReadyLimitStale},
		{name: "read error", next: 7, row: row, haveRow: true, readErr: readErr, status: StagedBodyReadyLimitReadError},
		{name: "staged missing", next: 7, row: row, haveRow: true, status: StagedBodyReadyLimitStagedMissing},
		{name: "hash mismatch", next: 7, row: row, haveRow: true, staged: rawdb.SyncStagedBlockRow{Number: 7, Hash: tcommon.Hash{0xff}}, haveStaged: true, status: StagedBodyReadyLimitHashMismatch},
		{name: "valid", next: 7, row: row, haveRow: true, staged: staged, haveStaged: true, status: StagedBodyReadyLimitValid, valid: true, limit: 7},
	}
	for _, tt := range tests {
		got := ValidateStagedBodyReadyDrainLimit(tt.next, tt.row, tt.haveRow, tt.staged, tt.haveStaged, tt.readErr)
		if got.Status != tt.status || got.Valid() != tt.valid || got.Limit != tt.limit {
			t.Fatalf("%s: result = %+v, want status %v valid %v limit %d", tt.name, got, tt.status, tt.valid, tt.limit)
		}
		if tt.readErr != nil && !errors.Is(got.ReadError, tt.readErr) {
			t.Fatalf("%s: ReadError = %v, want %v", tt.name, got.ReadError, tt.readErr)
		}
	}
}

func TestReadStagedBodyReadyDrainLimit(t *testing.T) {
	block := testBufferedBlock(7)
	tests := []struct {
		name    string
		setup   func(*testing.T, ethdb.KeyValueStore)
		next    uint64
		status  StagedBodyReadyLimitStatus
		valid   bool
		limit   uint64
		readErr bool
	}{
		{
			name:   "missing",
			next:   7,
			status: StagedBodyReadyLimitMissing,
		},
		{
			name: "valid",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
					t.Fatalf("write staged block: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			next:   block.Number(),
			status: StagedBodyReadyLimitValid,
			valid:  true,
			limit:  block.Number(),
		},
		{
			name: "unbound",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteStageProgress(db, rawdb.StageSyncBodiesReady, block.Number()); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			next:   block.Number(),
			status: StagedBodyReadyLimitUnbound,
		},
		{
			name: "stale without staged read",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block.Number()-1, tcommon.Hash{0x06}); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			next:   block.Number(),
			status: StagedBodyReadyLimitStale,
		},
		{
			name: "staged read error",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteSyncStagedBlockRaw(db, block, []byte{0x01, 0x02}); err != nil {
					t.Fatalf("write corrupt staged block: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			next:    block.Number(),
			status:  StagedBodyReadyLimitReadError,
			readErr: true,
		},
		{
			name: "hash mismatch",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
					t.Fatalf("write staged block: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block.Number(), tcommon.Hash{0xff}); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			next:   block.Number(),
			status: StagedBodyReadyLimitHashMismatch,
		},
	}
	for _, tt := range tests {
		db := rawdb.NewMemoryDatabase()
		if tt.setup != nil {
			tt.setup(t, db)
		}
		got := ReadStagedBodyReadyDrainLimit(db, tt.next)
		if got.Status != tt.status || got.Valid() != tt.valid || got.Limit != tt.limit {
			t.Fatalf("%s: result = %+v, want status %v valid %v limit %d", tt.name, got, tt.status, tt.valid, tt.limit)
		}
		if tt.readErr && got.ReadError == nil {
			t.Fatalf("%s: ReadError is nil, want staged read error", tt.name)
		}
	}
}

func TestReadStagedBodyReadyDrainLimitProgressReadError(t *testing.T) {
	got := ReadStagedBodyReadyDrainLimit(corruptStageProgressReader{}, 7)
	if got.Status != StagedBodyReadyLimitProgressReadError || got.StageError == nil {
		t.Fatalf("result = %+v, want progress read error", got)
	}
}

func stagedBodyMapReader(rows map[uint64]rawdb.SyncStagedBlockRow) StagedBodyReader {
	return func(number uint64) (rawdb.SyncStagedBlockRow, bool, error) {
		row, ok := rows[number]
		return row, ok, nil
	}
}

type corruptStageProgressReader struct{}

func (corruptStageProgressReader) Has([]byte) (bool, error) {
	return true, nil
}

func (corruptStageProgressReader) Get([]byte) ([]byte, error) {
	return []byte{0x01}, nil
}
