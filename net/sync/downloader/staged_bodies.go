package downloader

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// StagedBodyReader returns one persisted sync-staged body row by block number.
type StagedBodyReader func(number uint64) (rawdb.SyncStagedBlockRow, bool, error)

// StagedBodyReadyFrontier is the highest contiguous staged body reachable from
// a canonical head's next block.
type StagedBodyReadyFrontier struct {
	Have        bool
	Number      uint64
	Hash        tcommon.Hash
	ErrorAt     uint64
	Error       error
	NextMissing uint64
}

// StagedBodyReadyProgressRefresh records how SyncBodiesReady was refreshed
// from the current contiguous staged-body frontier.
type StagedBodyReadyProgressRefresh struct {
	Frontier    StagedBodyReadyFrontier
	Updated     bool
	Deleted     bool
	WriteError  error
	DeleteError error
}

// StagedBodyReadyAfterStageRefresh records the optional SyncBodiesReady refresh
// after one staged body was accepted.
type StagedBodyReadyAfterStageRefresh struct {
	ReadyLimit StagedBodyReadyLimit
	Refresh    StagedBodyReadyProgressRefresh
	Refreshed  bool
	Skipped    bool
}

// StagedBodyReadyLimitStatus explains whether a persisted SyncBodiesReady row
// is usable as the next local import drain limit.
type StagedBodyReadyLimitStatus uint8

const (
	StagedBodyReadyLimitMissing StagedBodyReadyLimitStatus = iota
	StagedBodyReadyLimitProgressReadError
	StagedBodyReadyLimitUnbound
	StagedBodyReadyLimitStale
	StagedBodyReadyLimitReadError
	StagedBodyReadyLimitStagedMissing
	StagedBodyReadyLimitHashMismatch
	StagedBodyReadyLimitValid
)

// StagedBodyReadyLimit is the validation result for a persisted
// SyncBodiesReady row.
type StagedBodyReadyLimit struct {
	Status     StagedBodyReadyLimitStatus
	Limit      uint64
	StageRow   rawdb.StageProgress
	StagedRow  rawdb.SyncStagedBlockRow
	StageError error
	ReadError  error
	StagedHash tcommon.Hash
}

// Valid reports whether the row can cap a local import drain.
func (l StagedBodyReadyLimit) Valid() bool {
	return l.Status == StagedBodyReadyLimitValid
}

// FindStagedBodyReadyFrontier scans staged bodies from start until the first
// gap, read error, target-head cap, or uint64 wrap. targetHead zero means no
// target cap is known.
func FindStagedBodyReadyFrontier(start, targetHead uint64, read StagedBodyReader) StagedBodyReadyFrontier {
	var frontier StagedBodyReadyFrontier
	if read == nil {
		frontier.NextMissing = start
		return frontier
	}
	expected := start
	for {
		if targetHead != 0 && expected > targetHead {
			frontier.NextMissing = expected
			return frontier
		}
		row, ok, err := read(expected)
		if err != nil {
			frontier.ErrorAt = expected
			frontier.Error = err
			frontier.NextMissing = expected
			return frontier
		}
		if !ok {
			frontier.NextMissing = expected
			return frontier
		}
		frontier.Have = true
		frontier.Number = row.Number
		frontier.Hash = row.Hash
		expected++
		if expected == 0 {
			frontier.NextMissing = expected
			return frontier
		}
	}
}

// RefreshStagedBodyReadyProgress recomputes SyncBodiesReady from the persisted
// sync-staged body table and writes or deletes the ready stage row.
func RefreshStagedBodyReadyProgress(db ethdb.KeyValueStore, start, targetHead uint64) StagedBodyReadyProgressRefresh {
	var result StagedBodyReadyProgressRefresh
	if db == nil {
		result.Frontier.NextMissing = start
		return result
	}
	frontier := FindStagedBodyReadyFrontier(start, targetHead, func(number uint64) (rawdb.SyncStagedBlockRow, bool, error) {
		return rawdb.ReadSyncStagedBlockRaw(db, number)
	})
	result.Frontier = frontier
	if !frontier.Have {
		result.Deleted = true
		result.DeleteError = rawdb.DeleteStageProgress(db, rawdb.StageSyncBodiesReady)
		return result
	}
	result.Updated = true
	result.WriteError = rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, frontier.Number, frontier.Hash)
	return result
}

// RefreshStagedBodyReadyProgressAfterStage refreshes SyncBodiesReady only when
// the newly staged body can start or extend the persisted contiguous frontier.
// Invalid existing ready rows still force a refresh so restart/drain state is
// repaired promptly.
func RefreshStagedBodyReadyProgressAfterStage(db ethdb.KeyValueStore, start, targetHead, stagedNum uint64) StagedBodyReadyAfterStageRefresh {
	var result StagedBodyReadyAfterStageRefresh
	if db == nil {
		result.Skipped = true
		result.ReadyLimit = StagedBodyReadyLimit{Status: StagedBodyReadyLimitMissing}
		return result
	}
	ready := ReadStagedBodyReadyDrainLimit(db, start)
	result.ReadyLimit = ready
	if !ShouldRefreshStagedBodyReadyAfterStage(start, targetHead, stagedNum, ready) {
		result.Skipped = true
		return result
	}
	result.Refreshed = true
	result.Refresh = RefreshStagedBodyReadyProgress(db, start, targetHead)
	return result
}

// ShouldRefreshStagedBodyReadyAfterStage reports whether a newly accepted
// staged body can change the persisted ready frontier.
func ShouldRefreshStagedBodyReadyAfterStage(start, targetHead, stagedNum uint64, ready StagedBodyReadyLimit) bool {
	switch ready.Status {
	case StagedBodyReadyLimitMissing:
		return stagedNum == start
	case StagedBodyReadyLimitValid:
		if targetHead != 0 && ready.Limit >= targetHead {
			return false
		}
		if ready.Limit == ^uint64(0) {
			return false
		}
		return stagedNum == ready.Limit+1
	default:
		return true
	}
}

// ReadStagedBodyReadyDrainLimit reads and validates the persisted
// SyncBodiesReady frontier that can cap one local staged-body import drain.
func ReadStagedBodyReadyDrainLimit(db ethdb.KeyValueReader, next uint64) StagedBodyReadyLimit {
	if db == nil {
		return ValidateStagedBodyReadyDrainLimit(next, rawdb.StageProgress{}, false, rawdb.SyncStagedBlockRow{}, false, nil)
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil {
		return StagedBodyReadyLimit{
			Status:     StagedBodyReadyLimitProgressReadError,
			StageError: err,
		}
	}
	var (
		staged   rawdb.SyncStagedBlockRow
		stagedOK bool
		readErr  error
	)
	if ok && row.HasBlockHash && row.BlockNum >= next {
		staged, stagedOK, readErr = rawdb.ReadSyncStagedBlockRaw(db, row.BlockNum)
	}
	return ValidateStagedBodyReadyDrainLimit(next, row, ok, staged, stagedOK, readErr)
}

// ValidateStagedBodyReadyDrainLimit checks that a persisted SyncBodiesReady
// row is hash-bound, not behind the next needed block, and still matches the
// staged body row it points at.
func ValidateStagedBodyReadyDrainLimit(next uint64, row rawdb.StageProgress, haveRow bool, staged rawdb.SyncStagedBlockRow, haveStaged bool, readErr error) StagedBodyReadyLimit {
	result := StagedBodyReadyLimit{
		StageRow:   row,
		StagedRow:  staged,
		ReadError:  readErr,
		StagedHash: staged.Hash,
	}
	if !haveRow {
		result.Status = StagedBodyReadyLimitMissing
		return result
	}
	if !row.HasBlockHash {
		result.Status = StagedBodyReadyLimitUnbound
		return result
	}
	if row.BlockNum < next {
		result.Status = StagedBodyReadyLimitStale
		return result
	}
	if readErr != nil {
		result.Status = StagedBodyReadyLimitReadError
		return result
	}
	if !haveStaged {
		result.Status = StagedBodyReadyLimitStagedMissing
		return result
	}
	if staged.Hash != row.BlockHash {
		result.Status = StagedBodyReadyLimitHashMismatch
		return result
	}
	result.Status = StagedBodyReadyLimitValid
	result.Limit = row.BlockNum
	return result
}
