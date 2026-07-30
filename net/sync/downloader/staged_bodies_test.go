package downloader

import (
	"bytes"
	"errors"
	"reflect"
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

func TestFindStagedBodyReadyFrontierRejectsNumberMismatch(t *testing.T) {
	rows := map[uint64]rawdb.SyncStagedBlockRow{
		3: {Number: 3, Hash: tcommon.Hash{0x03}},
		4: {Number: 9, Hash: tcommon.Hash{0x04}},
	}
	got := FindStagedBodyReadyFrontier(3, 0, stagedBodyMapReader(rows))
	if !got.Have || got.Number != 3 || got.ErrorAt != 4 || got.Error == nil || got.NextMissing != 4 {
		t.Fatalf("frontier = %+v, want block3 prefix and mismatch at 4", got)
	}
	if got.Error.Error() != "downloader: staged body reader returned block 9 for expected block 4" {
		t.Fatalf("frontier error = %v, want number mismatch", got.Error)
	}
}

func TestFindStagedBodyReadyFrontierWithoutReader(t *testing.T) {
	got := FindStagedBodyReadyFrontier(7, 0, nil)
	if got.Have || got.NextMissing != 7 || got.Error != nil {
		t.Fatalf("frontier = %+v, want empty next7", got)
	}
}

func TestStagedBodyAcceptanceFailureCoversStageProgressAndReady(t *testing.T) {
	stageErr := errors.New("stage write")
	progressReadErr := errors.New("progress read")
	progressWriteErr := errors.New("progress write")
	readyErr := errors.New("ready write")

	tests := []struct {
		name string
		in   StagedBodyAcceptance
		err  error
	}{
		{
			name: "stage write",
			in: StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
				StageError: stageErr,
			}},
			err: stageErr,
		},
		{
			name: "progress read",
			in: StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
				ProgressReadError: progressReadErr,
			}},
			err: progressReadErr,
		},
		{
			name: "progress write",
			in: StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
				ProgressWriteError: progressWriteErr,
			}},
			err: progressWriteErr,
		},
		{
			name: "ready refresh",
			in: StagedBodyAcceptance{Ready: StagedBodyReadyAfterStageRefresh{
				Refreshed: true,
				Refresh:   StagedBodyReadyProgressRefresh{WriteError: readyErr},
			}},
			err: readyErr,
		},
	}
	for _, tt := range tests {
		if !tt.in.Failed() || !errors.Is(tt.in.FailureError(), tt.err) {
			t.Fatalf("%s acceptance failed=%v err=%v, want %v", tt.name, tt.in.Failed(), tt.in.FailureError(), tt.err)
		}
	}

	skippedReady := StagedBodyAcceptance{Ready: StagedBodyReadyAfterStageRefresh{
		Refresh: StagedBodyReadyProgressRefresh{WriteError: readyErr},
		Skipped: true,
	}}
	if skippedReady.Failed() || skippedReady.FailureError() != nil {
		t.Fatalf("skipped-ready acceptance failed=%v err=%v, want success", skippedReady.Failed(), skippedReady.FailureError())
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

func TestRefreshStagedBodyReadyProgressAfterStage(t *testing.T) {
	tests := []struct {
		name      string
		start     uint64
		target    uint64
		stagedNum uint64
		setup     func(*testing.T, ethdb.KeyValueStore)
		refreshed bool
		skipped   bool
		frontier  uint64
		haveRow   bool
	}{
		{
			name:      "missing ready starts frontier",
			start:     2,
			stagedNum: 2,
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				writeTestStagedBlocks(t, db, 2, 3)
			},
			refreshed: true,
			frontier:  3,
			haveRow:   true,
		},
		{
			name:      "missing ready skips future body",
			start:     2,
			stagedNum: 4,
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				writeTestStagedBlocks(t, db, 4)
			},
			skipped: true,
		},
		{
			name:      "valid ready extends on next body",
			start:     2,
			stagedNum: 4,
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				writeTestStagedBlocks(t, db, 2, 3, 4)
				block3 := testBufferedBlock(3)
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, 3, block3.Hash()); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			refreshed: true,
			frontier:  4,
			haveRow:   true,
		},
		{
			name:      "valid ready skips gap body",
			start:     2,
			stagedNum: 5,
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				writeTestStagedBlocks(t, db, 2, 3, 5)
				block3 := testBufferedBlock(3)
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, 3, block3.Hash()); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			skipped:  true,
			frontier: 3,
			haveRow:  true,
		},
		{
			name:      "target cap skips extension",
			start:     2,
			target:    3,
			stagedNum: 4,
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				writeTestStagedBlocks(t, db, 2, 3, 4)
				block3 := testBufferedBlock(3)
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, 3, block3.Hash()); err != nil {
					t.Fatalf("write ready progress: %v", err)
				}
			},
			skipped:  true,
			frontier: 3,
			haveRow:  true,
		},
		{
			name:      "invalid ready row forces repair",
			start:     2,
			stagedNum: 5,
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				writeTestStagedBlocks(t, db, 2, 3)
				if err := rawdb.WriteStageProgress(db, rawdb.StageSyncBodiesReady, 9); err != nil {
					t.Fatalf("write unbound ready progress: %v", err)
				}
			},
			refreshed: true,
			frontier:  3,
			haveRow:   true,
		},
	}
	for _, tt := range tests {
		db := rawdb.NewMemoryDatabase()
		if tt.setup != nil {
			tt.setup(t, db)
		}
		got := RefreshStagedBodyReadyProgressAfterStage(db, tt.start, tt.target, tt.stagedNum)
		if got.Refreshed != tt.refreshed || got.Skipped != tt.skipped {
			t.Fatalf("%s: result = %+v, want refreshed %v skipped %v", tt.name, got, tt.refreshed, tt.skipped)
		}
		row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
		if err != nil {
			t.Fatalf("%s: read ready progress: %v", tt.name, err)
		}
		if ok != tt.haveRow {
			t.Fatalf("%s: ready row ok=%v, want %v", tt.name, ok, tt.haveRow)
		}
		if tt.haveRow && row.BlockNum != tt.frontier {
			t.Fatalf("%s: ready row = %+v, want frontier %d", tt.name, row, tt.frontier)
		}
	}
}

func TestAcceptStagedBodyStagesBodyAndRefreshesReadyFrontier(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)

	first := AcceptStagedBody(db, block3, nil, 2, 0)
	if first.Write.StageError != nil || !first.Write.Staged || !first.Write.ProgressWritten {
		t.Fatalf("first accept = %+v, want staged block3 and SyncBodies progress", first)
	}
	if !first.Ready.Skipped || first.Ready.Refreshed {
		t.Fatalf("first ready refresh = %+v, want skipped gapped body", first.Ready)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("ready after block3 = %+v ok=%v err=%v, want absent", row, ok, err)
	}

	second := AcceptStagedBody(db, block2, nil, 2, 0)
	if second.Write.StageError != nil || !second.Write.Staged || !second.Write.ProgressSkipped {
		t.Fatalf("second accept = %+v, want staged block2 without regressing SyncBodies", second)
	}
	if !second.Ready.Refreshed || second.Ready.Skipped || second.Ready.Refresh.WriteError != nil {
		t.Fatalf("second ready refresh = %+v, want refreshed", second.Ready)
	}
	if !second.Ready.Refresh.Frontier.Have || second.Ready.Refresh.Frontier.Number != block3.Number() || second.Ready.Refresh.Frontier.Hash != block3.Hash() {
		t.Fatalf("ready frontier = %+v, want block3", second.Ready.Refresh.Frontier)
	}
	bodies, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodies)
	if err != nil || !ok || bodies.BlockNum != block3.Number() || bodies.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodies = %+v ok=%v err=%v, want block3", bodies, ok, err)
	}
	ready, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block3.Number() || ready.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodiesReady = %+v ok=%v err=%v, want block3", ready, ok, err)
	}
	for _, block := range []*types.Block{block2, block3} {
		if row, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block.Number()); err != nil || !ok || row.Hash != block.Hash() {
			t.Fatalf("staged block %d = %+v ok=%v err=%v, want persisted", block.Number(), row, ok, err)
		}
	}
}

func TestAcceptStagedBodyStopsBeforeReadyRefreshOnStageError(t *testing.T) {
	got := AcceptStagedBody(nil, testBufferedBlock(2), nil, 2, 0)
	if got.Write.StageError == nil {
		t.Fatalf("accept with nil db = %+v, want stage error", got)
	}
	if got.Ready.Refreshed || got.Ready.Skipped || got.Ready.ReadyLimit.Status != StagedBodyReadyLimitMissing {
		t.Fatalf("ready refresh after stage error = %+v, want untouched zero result", got.Ready)
	}
}

func TestAcceptStagedBodyStopsBeforeReadyRefreshOnProgressReadError(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	db := &corruptStageProgressStore{
		KeyValueStore: base,
		stage:         rawdb.StageSyncBodies,
	}
	block := testBufferedBlock(2)

	got := AcceptStagedBody(db, block, nil, 2, 0)
	if got.Write.StageError != nil || got.Write.ProgressReadError == nil || got.Write.ProgressWriteError != nil {
		t.Fatalf("accept = %+v, want only progress read error", got)
	}
	if !got.Write.Staged || got.Write.ProgressWritten || got.Write.ProgressSkipped {
		t.Fatalf("accept write = %+v, want staged body without progress", got.Write)
	}
	if got.Ready.Refreshed || got.Ready.Skipped || got.Ready.ReadyLimit.Status != StagedBodyReadyLimitMissing {
		t.Fatalf("ready refresh after progress read error = %+v, want untouched zero result", got.Ready)
	}
	if row, ok, err := rawdb.ReadSyncStagedBlockRaw(base, block.Number()); err != nil || !ok || row.Hash != block.Hash() {
		t.Fatalf("staged block = %+v ok=%v err=%v, want persisted block2", row, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(base, rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after progress read error = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestPruneStaleStagedBodyTailRefreshesReadyFrontier(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block5 := testBufferedBlock(5)
	for _, block := range []*types.Block{block2, block3, block5} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block5.Number(), block5.Hash()); err != nil {
		t.Fatalf("write SyncBodies: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block5.Number(), block5.Hash()); err != nil {
		t.Fatalf("write stale SyncBodiesReady: %v", err)
	}

	got := PruneStaleStagedBodyTail(db, 4, block3.Number(), block3.Hash(), true, 2, 0)
	if got.PruneError != nil || got.Prune.Deleted != 1 || !got.Prune.RewoundProgress || got.Prune.RewindBlock != block3.Number() {
		t.Fatalf("prune result = %+v, want delete block5 and rewind SyncBodies to block3", got)
	}
	if !got.Ready.Updated || got.Ready.DeleteError != nil || got.Ready.WriteError != nil {
		t.Fatalf("ready refresh = %+v, want updated", got.Ready)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block5.Number()); err != nil || ok {
		t.Fatalf("stale staged block5 ok=%v err=%v, want deleted", ok, err)
	}
	bodies, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodies)
	if err != nil || !ok || bodies.BlockNum != block3.Number() || bodies.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodies = %+v ok=%v err=%v, want block3", bodies, ok, err)
	}
	ready, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block3.Number() || ready.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodiesReady = %+v ok=%v err=%v, want block3", ready, ok, err)
	}
}

func TestDeleteImportedStagedBodiesThroughRefreshesReadyFrontier(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block4 := testBufferedBlock(4)
	for _, block := range []*types.Block{block2, block3, block4} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block2.Number(), block2.Hash()); err != nil {
		t.Fatalf("write stale SyncBodiesReady: %v", err)
	}

	got := DeleteImportedStagedBodiesThrough(db, block2.Number(), block3.Number(), 0)
	if got.DeleteError != nil || got.Deleted != 1 {
		t.Fatalf("cleanup result = %+v, want one deleted body without error", got)
	}
	if !got.Ready.Updated || got.Ready.DeleteError != nil || got.Ready.WriteError != nil {
		t.Fatalf("ready refresh = %+v, want updated", got.Ready)
	}
	if !got.Ready.Frontier.Have || got.Ready.Frontier.Number != block4.Number() || got.Ready.Frontier.Hash != block4.Hash() {
		t.Fatalf("ready frontier = %+v, want block4", got.Ready.Frontier)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block2.Number()); err != nil || ok {
		t.Fatalf("imported staged block2 ok=%v err=%v, want deleted", ok, err)
	}
	for _, block := range []*types.Block{block3, block4} {
		if row, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block.Number()); err != nil || !ok || row.Hash != block.Hash() {
			t.Fatalf("remaining staged block %d = %+v ok=%v err=%v, want present", block.Number(), row, ok, err)
		}
	}
	ready, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block4.Number() || ready.BlockHash != block4.Hash() {
		t.Fatalf("SyncBodiesReady = %+v ok=%v err=%v, want block4", ready, ok, err)
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
		{name: "number mismatch", next: 7, row: row, haveRow: true, staged: rawdb.SyncStagedBlockRow{Number: 8, Hash: row.BlockHash}, haveStaged: true, status: StagedBodyReadyLimitNumberMismatch},
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

func TestValidateStagedBodyProgress(t *testing.T) {
	readErr := errors.New("read staged")
	row := rawdb.StageProgress{
		Stage:        rawdb.StageSyncBodies,
		BlockNum:     7,
		BlockHash:    tcommon.Hash{0x07},
		HasBlockHash: true,
	}
	staged := rawdb.SyncStagedBlockRow{Number: 7, Hash: row.BlockHash}
	tests := []struct {
		name       string
		row        rawdb.StageProgress
		haveRow    bool
		staged     rawdb.SyncStagedBlockRow
		haveStaged bool
		readErr    error
		status     StagedBodyProgressStatus
		valid      bool
	}{
		{name: "missing", status: StagedBodyProgressMissing},
		{name: "unbound", row: rawdb.StageProgress{Stage: rawdb.StageSyncBodies, BlockNum: 7}, haveRow: true, status: StagedBodyProgressUnbound},
		{name: "read error", row: row, haveRow: true, readErr: readErr, status: StagedBodyProgressStagedReadError},
		{name: "staged missing", row: row, haveRow: true, status: StagedBodyProgressStagedMissing},
		{name: "number mismatch", row: row, haveRow: true, staged: rawdb.SyncStagedBlockRow{Number: 8, Hash: row.BlockHash}, haveStaged: true, status: StagedBodyProgressNumberMismatch},
		{name: "hash mismatch", row: row, haveRow: true, staged: rawdb.SyncStagedBlockRow{Number: 7, Hash: tcommon.Hash{0xff}}, haveStaged: true, status: StagedBodyProgressHashMismatch},
		{name: "valid", row: row, haveRow: true, staged: staged, haveStaged: true, status: StagedBodyProgressValid, valid: true},
	}
	for _, tt := range tests {
		got := ValidateStagedBodyProgress(tt.row, tt.haveRow, tt.staged, tt.haveStaged, tt.readErr)
		if got.Status != tt.status || got.Valid() != tt.valid {
			t.Fatalf("%s: result = %+v, want status %v valid %v", tt.name, got, tt.status, tt.valid)
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

func TestReadStagedBodyProgress(t *testing.T) {
	block := testBufferedBlock(7)
	tests := []struct {
		name   string
		setup  func(*testing.T, ethdb.KeyValueStore)
		status StagedBodyProgressStatus
		valid  bool
	}{
		{
			name:   "missing",
			status: StagedBodyProgressMissing,
		},
		{
			name: "valid",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
					t.Fatalf("write staged block: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write stage progress: %v", err)
				}
			},
			status: StagedBodyProgressValid,
			valid:  true,
		},
		{
			name: "unbound",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteStageProgress(db, rawdb.StageSyncBodies, block.Number()); err != nil {
					t.Fatalf("write stage progress: %v", err)
				}
			},
			status: StagedBodyProgressUnbound,
		},
		{
			name: "staged missing",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write stage progress: %v", err)
				}
			},
			status: StagedBodyProgressStagedMissing,
		},
		{
			name: "staged read error",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteSyncStagedBlockRaw(db, block, []byte{0x01, 0x02}); err != nil {
					t.Fatalf("write corrupt staged block: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write stage progress: %v", err)
				}
			},
			status: StagedBodyProgressStagedReadError,
		},
		{
			name: "hash mismatch",
			setup: func(t *testing.T, db ethdb.KeyValueStore) {
				t.Helper()
				if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
					t.Fatalf("write staged block: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block.Number(), tcommon.Hash{0xff}); err != nil {
					t.Fatalf("write stage progress: %v", err)
				}
			},
			status: StagedBodyProgressHashMismatch,
		},
	}
	for _, tt := range tests {
		db := rawdb.NewMemoryDatabase()
		if tt.setup != nil {
			tt.setup(t, db)
		}
		got := ReadStagedBodyProgress(db, rawdb.StageSyncBodies)
		if got.Status != tt.status || got.Valid() != tt.valid {
			t.Fatalf("%s: result = %+v, want status %v valid %v", tt.name, got, tt.status, tt.valid)
		}
	}
}

func TestReadStagedBodyReadyDrainLimitProgressReadError(t *testing.T) {
	got := ReadStagedBodyReadyDrainLimit(corruptStageProgressReader{}, 7)
	if got.Status != StagedBodyReadyLimitProgressReadError || got.StageError == nil {
		t.Fatalf("result = %+v, want progress read error", got)
	}
}

func TestReadStagedBodyProgressReadError(t *testing.T) {
	got := ReadStagedBodyProgress(corruptStageProgressReader{}, rawdb.StageSyncBodies)
	if got.Status != StagedBodyProgressReadError || got.StageError == nil {
		t.Fatalf("result = %+v, want progress read error", got)
	}
}

func TestReadStagedBodyDrainPlan(t *testing.T) {
	block := testBufferedBlock(9)
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
		t.Fatalf("write staged block: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block.Number(), block.Hash()); err != nil {
		t.Fatalf("write ready progress: %v", err)
	}

	got := ReadStagedBodyDrainPlan(db, 7, 10)
	if got.Ready.Status != StagedBodyReadyLimitValid || got.Ready.Limit != block.Number() {
		t.Fatalf("ready = %+v, want valid block9", got.Ready)
	}
	if !got.Plan.HasReadyLimit || got.Plan.ReadyLimit != block.Number() || !got.Plan.CanDrain || got.Plan.RestoreLimit != 3 || got.Plan.RefreshReady {
		t.Fatalf("plan = %+v, want clamped drain 7..9 without refresh", got.Plan)
	}
	if len(got.Plan.Steps) != 2 || got.Plan.Steps[0].Action != StagedBodyDrainRestoreBodies || got.Plan.Steps[0].From != 7 || got.Plan.Steps[0].Limit != 3 || got.Plan.Steps[1].Action != StagedBodyDrainPopBuffer || got.Plan.Steps[1].Next != 7 || got.Plan.Steps[1].Limit != 3 {
		t.Fatalf("steps = %+v, want restore/pop 7 limit 3", got.Plan.Steps)
	}
}

func TestReadStagedBodyDrainPlanRefreshesInvalidReadyBeforeDrain(t *testing.T) {
	block := testBufferedBlock(7)
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
		t.Fatalf("write staged block: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block.Number(), tcommon.Hash{0xff}); err != nil {
		t.Fatalf("write mismatched ready progress: %v", err)
	}

	got := ReadStagedBodyDrainPlan(db, block.Number(), 5)
	if got.Ready.Status != StagedBodyReadyLimitHashMismatch {
		t.Fatalf("ready = %+v, want hash mismatch", got.Ready)
	}
	if got.Plan.HasReadyLimit || got.Plan.ReadyLimit != 0 || !got.Plan.RefreshReady || !got.Plan.CanDrain || got.Plan.RestoreLimit != 5 {
		t.Fatalf("plan = %+v, want refresh plus uncapped local drain", got.Plan)
	}
	if len(got.Plan.Steps) != 3 || got.Plan.Steps[0].Action != StagedBodyDrainRefreshReady || got.Plan.Steps[1].Action != StagedBodyDrainRestoreBodies || got.Plan.Steps[2].Action != StagedBodyDrainPopBuffer {
		t.Fatalf("steps = %+v, want refresh/restore/pop", got.Plan.Steps)
	}
}

func TestReadAndApplyStagedBodyDrainPlan(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block7 := testBufferedBlock(7)
	block8 := testBufferedBlock(8)
	for _, block := range []*types.Block{block7, block8} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block8.Number(), block8.Hash()); err != nil {
		t.Fatalf("write ready progress: %v", err)
	}
	popBatch := BufferedBatch{Buffered: []BufferedBlock{
		{Num: block7.Number(), Hash: block7.Hash()},
		{Num: block8.Number(), Hash: block8.Hash()},
	}}
	applier := &recordingStagedBodyDrainApplier{popBatch: popBatch}

	got := ReadAndApplyStagedBodyDrainPlan(db, block7.Number(), 10, applier)

	if got.Read.Ready.Status != StagedBodyReadyLimitValid || got.Read.Ready.Limit != block8.Number() {
		t.Fatalf("ready = %+v, want valid block8 frontier", got.Read.Ready)
	}
	wantPlan := StagedBodyDrainPlan{
		RestoreLimit:  2,
		CanDrain:      true,
		ReadyLimit:    block8.Number(),
		HasReadyLimit: true,
		Steps: []StagedBodyDrainStep{
			{Action: StagedBodyDrainRestoreBodies, From: block7.Number(), Limit: 2},
			{Action: StagedBodyDrainPopBuffer, Next: block7.Number(), Limit: 2},
		},
	}
	if !reflect.DeepEqual(got.Read.Plan, wantPlan) {
		t.Fatalf("plan = %+v, want %+v", got.Read.Plan, wantPlan)
	}
	wantCalls := []recordedStagedBodyDrainCall{
		{action: StagedBodyDrainRestoreBodies, from: block7.Number(), limit: 2},
		{action: StagedBodyDrainPopBuffer, next: block7.Number(), limit: 2},
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	if !got.Apply.HasStagedBodyRestore {
		t.Fatalf("apply = %+v, want restore result marker", got.Apply)
	}
	if !reflect.DeepEqual(got.Batch, popBatch) || !reflect.DeepEqual(got.Apply.Batch, popBatch) {
		t.Fatalf("batch = %+v apply=%+v, want %+v", got.Batch, got.Apply.Batch, popBatch)
	}

	nilApplied := ReadAndApplyStagedBodyDrainPlan(db, block7.Number(), 10, nil)
	if nilApplied.Read.Ready.Status != StagedBodyReadyLimitValid || len(nilApplied.Apply.AppliedSteps) != 0 || len(nilApplied.Batch.Buffered) != 0 {
		t.Fatalf("nil applier result = %+v, want read plan without side effects", nilApplied)
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

type corruptStageProgressStore struct {
	ethdb.KeyValueStore
	stage rawdb.StageID
}

func (db *corruptStageProgressStore) Has(key []byte) (bool, error) {
	if bytes.Equal(key, stageProgressTestKey(db.stage)) {
		return true, nil
	}
	return db.KeyValueStore.Has(key)
}

func (db *corruptStageProgressStore) Get(key []byte) ([]byte, error) {
	if bytes.Equal(key, stageProgressTestKey(db.stage)) {
		return []byte{0x01}, nil
	}
	return db.KeyValueStore.Get(key)
}

func stageProgressTestKey(stage rawdb.StageID) []byte {
	return []byte("stage-progress-v1-" + string(stage))
}

func writeTestStagedBlocks(t *testing.T, db ethdb.KeyValueStore, nums ...uint64) {
	t.Helper()
	for _, num := range nums {
		block := testBufferedBlock(int64(num))
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", num, err)
		}
	}
}
