package downloader

import (
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
