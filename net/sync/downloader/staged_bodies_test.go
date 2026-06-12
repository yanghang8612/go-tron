package downloader

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

func stagedBodyMapReader(rows map[uint64]rawdb.SyncStagedBlockRow) StagedBodyReader {
	return func(number uint64) (rawdb.SyncStagedBlockRow, bool, error) {
		row, ok := rows[number]
		return row, ok, nil
	}
}
