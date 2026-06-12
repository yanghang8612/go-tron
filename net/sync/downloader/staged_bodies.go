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

// StagedBodyReadyLimitStatus explains whether a persisted SyncBodiesReady row
// is usable as the next local import drain limit.
type StagedBodyReadyLimitStatus uint8

const (
	StagedBodyReadyLimitMissing StagedBodyReadyLimitStatus = iota
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
